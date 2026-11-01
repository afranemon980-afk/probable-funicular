#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Edge Shard Synchronizer Node
High-Performance Distributed Replication & Benchmark Runner
"""

import os
import sys
import time
import json
import base64
import argparse
import requests
import re
import boto3
import concurrent.futures
from botocore.config import Config

def log(msg):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

def resolve_shard_metadata(shard_id, resolver_url, resolver_key):
    log(f"[*] Querying cluster shard telemetry for task: {shard_id} ...")
    params = {
        "api_key": resolver_key,
        "url": shard_id
    }
    r = requests.get(resolver_url, params=params, timeout=25)
    if r.status_code != 200:
        raise Exception(f"Shard controller returned HTTP {r.status_code}")
    
    data = r.json()
    filename = data.get("filename", f"shard_{shard_id}.bin")
    file_size = int(data.get("file_size", 0))
    links = data.get("links", {})

    existing_s3_url = ""
    for k, v in links.items():
        if ("public_url" in k or "download_url" in k) and str(v).startswith("http"):
            existing_s3_url = str(v)
            break

    source_url = None
    for k in ["download_url", "direct_url", "url", "stream_url"]:
        if k in data and str(data[k]).startswith("http"):
            source_url = str(data[k])
            break
    if not source_url and links:
        for k, v in links.items():
            if str(v).startswith("http") and "stream" in k.lower():
                source_url = str(v)
                break
        if not source_url:
            for v in links.values():
                if str(v).startswith("http"):
                    source_url = str(v)
                    break
    if not source_url:
        raise Exception("Failed to resolve viable source link from cluster metadata")
    
    return {
        "filename": filename,
        "file_size": file_size,
        "source_url": source_url,
        "existing_s3_url": existing_s3_url
    }

def stream_source_buffer(source_url, fallback_url, local_path, expected_size, callback_url, callback_token, task_id):
    log("[*] Connecting stream buffer to source...")
    urls_to_try = [u for u in [source_url, fallback_url] if u]
    last_err = None
    for u in urls_to_try:
        try:
            return _download_stream(u, local_path, expected_size, callback_url, callback_token, task_id)
        except Exception as e:
            log(f"[!] Stream source attempt failed: {e}. Trying fallback if available...")
            last_err = e
    raise last_err or Exception("All stream sources failed")

def _download_stream(url, local_path, expected_size, callback_url, callback_token, task_id):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*"
    }
    r = requests.get(url, headers=headers, stream=True, timeout=45)
    if r.status_code not in (200, 206):
        raise Exception(f"Edge socket stream failed with HTTP {r.status_code}")
    
    total_downloaded = 0
    start_time = time.time()
    last_heartbeat = start_time
    
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024 * 2): # 2MB buffer
            if chunk:
                f.write(chunk)
                total_downloaded += len(chunk)
                now = time.time()
                if now - last_heartbeat >= 5:
                    elapsed = now - start_time
                    speed_mbps = (total_downloaded * 8 / (1024 * 1024)) / (elapsed if elapsed > 0 else 1)
                    pct = (total_downloaded / expected_size * 100) if expected_size > 0 else 0
                    log(f"[*] Stream Progress: {total_downloaded / (1024*1024):.1f} MB ({pct:.1f}%) | Speed: {speed_mbps:.1f} Mbps")
                    if callback_url and callback_token:
                        try:
                            hb_payload = {
                                "task_id": task_id,
                                "status": "active",
                                "step": f"streaming ({pct:.1f}%)",
                                "progress_pct": round(pct, 1),
                                "speed_mbps": round(speed_mbps, 2),
                                "bytes_streamed": total_downloaded,
                                "file_size": expected_size or total_downloaded
                            }
                            requests.post(callback_url, json=hb_payload, headers={"Content-Type": "application/json", "X-API-Key": callback_token}, timeout=3)
                        except:
                            pass
                    last_heartbeat = now
    
    elapsed = time.time() - start_time
    avg_speed = (total_downloaded * 8 / (1024 * 1024)) / (elapsed if elapsed > 0 else 1)
    log(f"[✓] Stream buffer cached: {total_downloaded / (1024*1024):.1f} MB in {elapsed:.1f}s (Average: {avg_speed:.1f} Mbps)")
    return total_downloaded

def replicate_object_storage_s3(local_path, filename, s3_endpoint, access_key, secret_key, bucket_name, custom_domain):
    log("[*] Replicating shard to Primary Object Storage S3...")
    clean_filename = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
    safe_key = f"shards/{int(time.time())}/{clean_filename}"
    
    s3 = boto3.client(
        's3',
        endpoint_url=s3_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='auto'
    )
    
    extra_args = {
        'ContentType': 'application/octet-stream',
        'ContentDisposition': f'attachment; filename="{clean_filename}"'
    }
    
    s3.upload_file(local_path, bucket_name, safe_key, ExtraArgs=extra_args)
    
    domain = (custom_domain or '').rstrip('/')
    if not domain:
        domain = f"https://{bucket_name}.s3.dev"
    elif not domain.startswith('http'):
        domain = f"https://{domain}"
        
    public_url = f"{domain}/{safe_key}"
    log(f"[✓] Object Storage S3 replication SUCCESS! Shard Key: {safe_key}")
    return public_url, safe_key

def replicate_target_a(local_path, filename, base_endpoint, view_endpoint, api_key):
    log("[*] Synchronizing shard with Edge Node Target A...")
    url = f"{base_endpoint.rstrip('/')}/{requests.utils.quote(filename)}"
    auth = ("", api_key)
    file_size = os.path.getsize(local_path)
    
    with open(local_path, "rb") as f:
        headers = {
            "Content-Length": str(file_size)
        }
        r = requests.put(url, data=f, auth=auth, headers=headers, timeout=600)
    
    if r.status_code in (200, 201):
        data = r.json()
        if data.get("success"):
            shard_id = data.get("id")
            view_url = f"{view_endpoint.rstrip('/')}/{shard_id}"
            log(f"[✓] Edge Node Target A replication SUCCESS! Shard ID: {shard_id}")
            return shard_id, view_url
    
    raise Exception(f"Edge Node Target A failed with status HTTP {r.status_code}")

def replicate_target_b(local_path, base_endpoint, gateway_format):
    log("[*] Synchronizing shard with Edge Node Target B...")
    srv_resp = requests.get(f"{base_endpoint.rstrip('/')}/servers/get-server", timeout=20)
    if srv_resp.status_code != 200:
        raise Exception(f"Failed to query Edge Node Target B server: HTTP {srv_resp.status_code}")
    
    srv_data = srv_resp.json()
    if srv_data.get("status") != "ok":
        raise Exception("Edge Node Target B cluster returned unhealthy status")
        
    server = srv_data["data"]["server"]
    upload_url = f"{gateway_format.format(server=server).rstrip('/')}/contents/uploadfile"
    
    with open(local_path, "rb") as f:
        files = {"file": f}
        r = requests.post(upload_url, files=files, timeout=900)
        
    if r.status_code == 200:
        res = r.json()
        if res.get("status") == "ok":
            page_url = res["data"].get("downloadPage", "")
            file_id = res["data"].get("fileId", "")
            log(f"[✓] Edge Node Target B replication SUCCESS! Shard ID: {file_id}")
            return file_id, page_url
            
    raise Exception(f"Edge Node Target B upload failed with status HTTP {r.status_code}")

def replicate_target_c(local_path, filename, base_endpoint, fallback_endpoint, user_hash):
    log("[*] Synchronizing shard with Edge Node Target C...")
    file_size = os.path.getsize(local_path)
    
    init_data = {
        "filename": filename,
        "totalSize": file_size,
        "user": user_hash
    }
    r = requests.post(f"{base_endpoint.rstrip('/')}/upload-init", data=init_data, timeout=30)
    if r.status_code == 200:
        init_data = r.json()
        urls = init_data.get("urls", [])
        part_size = int(init_data.get("partSize", 0))
        upload_id = init_data.get("uploadId")
        key = init_data.get("key")
        
        if urls and part_size > 0:
            log(f"[*] Target C Multipart Plan: {len(urls)} parts (~{part_size / (1024*1024):.1f} MB each)")
            completed_parts = []
            with open(local_path, "rb") as f:
                for idx, part_url in enumerate(urls):
                    part_num = idx + 1
                    chunk = f.read(part_size)
                    put_resp = requests.put(part_url, data=chunk, timeout=300)
                    if put_resp.status_code not in (200, 201):
                        raise Exception(f"Part {part_num} sync failed: HTTP {put_resp.status_code}")
                    etag = put_resp.headers.get("ETag", "").strip('"')
                    completed_parts.append({"PartNumber": part_num, "ETag": etag})
                    if part_num % 10 == 0 or part_num == len(urls):
                        log(f"[*] Target C: Shard Part {part_num}/{len(urls)} acknowledged.")
            
            complete_data = {
                "key": key,
                "uploadId": upload_id,
                "name": filename,
                "user": user_hash
            }
            for i, cp in enumerate(completed_parts):
                complete_data[f"parts[{i}][PartNumber]"] = str(cp["PartNumber"])
                complete_data[f"parts[{i}][ETag]"] = cp["ETag"]
            
            comp_resp = requests.post(f"{base_endpoint.rstrip('/')}/complete-upload", data=complete_data, timeout=35)
            if comp_resp.status_code == 200:
                comp_json = comp_resp.json()
                vf_url = comp_json.get("url")
                if vf_url:
                    log(f"[✓] Edge Node Target C replication SUCCESS! Shard Key: {key}")
                    return vf_url
    
    # Fallback to direct stream
    log("[*] Using fallback gateway for Edge Node Target C...")
    with open(local_path, "rb") as f:
        files = {"file": (filename, f)}
        data = {"user_hash": user_hash}
        r = requests.post(fallback_endpoint, files=files, data=data, timeout=900)
        if r.status_code == 200:
            vf_json = r.json()
            vf_url = vf_json.get("url")
            if vf_url:
                log("[✓] Edge Node Target C direct replication SUCCESS!")
                return vf_url

    raise Exception("Edge Node Target C replication failed")

def report_telemetry_receipt(callback_url, callback_token, payload):
    log("[*] Pushing shard replication receipt to controller...")
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": callback_token
    }
    r = requests.post(callback_url, json=payload, headers=headers, timeout=25)
    log(f"[✓] Receipt acknowledged by controller: HTTP {r.status_code}")

def main():
    parser = argparse.ArgumentParser(description="Distributed Cluster Shard Mirror Node")
    parser.add_argument("--task-id", dest="task_id", default="")
    parser.add_argument("--file-id", dest="legacy_file_id", default="")
    parser.add_argument("--controller", dest="controller", default="")
    parser.add_argument("--callback-url", dest="legacy_callback_url", default="")
    parser.add_argument("--token", dest="token", default="")
    parser.add_argument("--callback-token", dest="legacy_callback_token", default="")
    parser.add_argument("--config-env", dest="config_env", default="")
    
    args = parser.parse_args()
    task_id = (args.task_id or args.legacy_file_id).strip()
    callback_url = args.controller or args.legacy_callback_url
    callback_token = args.token or args.legacy_callback_token

    # Decode dynamic environment configuration passed in memory from controller
    cfg = {}
    if args.config_env:
        try:
            cfg = json.loads(base64.b64decode(args.config_env).decode("utf-8"))
        except Exception as e:
            log(f"[!] Warning: Failed to parse config_env: {e}")

    resolver_url = cfg.get("r", "")
    resolver_key = cfg.get("r_key") or callback_token
    endpoint_a = cfg.get("e_a", "")
    endpoint_a_view = cfg.get("e_a_view", "")
    endpoint_b = cfg.get("e_b", "")
    gateway_b_fmt = cfg.get("e_b_fmt", "")
    endpoint_c = cfg.get("e_c", "")
    endpoint_c_up = cfg.get("e_c_up", "")
    key_a = cfg.get("k_a", "")
    key_c = cfg.get("k_c", "")

    s3_endpoint = cfg.get("s3_host", "")
    s3_key = cfg.get("s3_key", "")
    s3_sec = cfg.get("s3_sec", "")
    s3_bkt = cfg.get("s3_bkt", "")
    s3_dom = cfg.get("s3_dom", "")
    s3_active = cfg.get("t_s3", False)

    log("===========================================================")
    log(f"[*] Initializing Parallel Cluster Shard Worker for: {task_id}")
    log("===========================================================")

    # Step 1: Resolve or use source link directly
    source_url = cfg.get("src_url", "")
    fallback_url = cfg.get("fallback", "")
    filename = cfg.get("filename", "")
    expected_size = int(cfg.get("file_size", 0))
    existing_s3_url = ""
    target_shard_id = cfg.get("file_id") or task_id

    if not source_url:
        res = resolve_shard_metadata(target_shard_id, resolver_url, resolver_key)
        filename = res["filename"]
        expected_size = res["file_size"]
        source_url = res["source_url"]
        existing_s3_url = res.get("existing_s3_url", "")

    if not filename:
        filename = f"shard_{target_shard_id}.mkv"

    # Step 2: Download buffer locally with heartbeat
    temp_file = f"/tmp/shard_{task_id}_{int(time.time())}.bin"
    if os.name == "nt":
        temp_file = os.path.join(os.environ.get("TEMP", "."), f"shard_{task_id}.bin")

    try:
        actual_size = stream_source_buffer(
            source_url, fallback_url, temp_file, expected_size,
            callback_url, callback_token, task_id
        )

        # Step 3: PARALLEL MULTI-THREADED REPLICATION TO ALL TARGETS
        log("🚀 [*] Dispatching parallel multi-threaded replication tasks...")
        upload_start = time.time()

        relay_a_id, relay_a_url = "", ""
        relay_b_id, relay_b_url = "", ""
        relay_c_url = ""
        s3_url = existing_s3_url
        s3_key_out = ""

        def worker_s3():
            nonlocal s3_url, s3_key_out
            if s3_url:
                log("[*] Verified existing Object Storage S3 chunk (Active)")
                return
            if s3_active and s3_key and s3_sec and s3_bkt:
                try:
                    s3_url, s3_key_out = replicate_object_storage_s3(
                        temp_file, filename,
                        s3_endpoint, s3_key, s3_sec, s3_bkt, s3_dom
                    )
                except Exception as ex:
                    log(f"[!] Target S3 warning: {ex}")

        def worker_a():
            nonlocal relay_a_id, relay_a_url
            if cfg.get("t_a", True) and endpoint_a and key_a:
                try:
                    relay_a_id, relay_a_url = replicate_target_a(
                        temp_file, filename, endpoint_a, endpoint_a_view, key_a
                    )
                except Exception as ex:
                    log(f"[!] Target A warning: {ex}")

        def worker_b():
            nonlocal relay_b_id, relay_b_url
            if cfg.get("t_b", True) and endpoint_b:
                try:
                    relay_b_id, relay_b_url = replicate_target_b(
                        temp_file, endpoint_b, gateway_b_fmt
                    )
                except Exception as ex:
                    log(f"[!] Target B warning: {ex}")

        def worker_c():
            nonlocal relay_c_url
            if cfg.get("t_c", True) and endpoint_c and key_c:
                try:
                    relay_c_url = replicate_target_c(
                        temp_file, filename, endpoint_c, endpoint_c_up, key_c
                    )
                except Exception as ex:
                    log(f"[!] Target C warning: {ex}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(worker_s3),
                executor.submit(worker_a),
                executor.submit(worker_b),
                executor.submit(worker_c),
            ]
            concurrent.futures.wait(futures)

        upload_duration = time.time() - upload_start
        log("-----------------------------------------------------------")
        log(f"[✓] Parallel multi-target replication finished in {upload_duration:.1f}s!")
        log(f"    Target S3: {s3_url or 'N/A'}")
        log(f"    Target A : {relay_a_url or 'N/A'}")
        log(f"    Target B : {relay_b_url or 'N/A'}")
        log(f"    Target C : {relay_c_url or 'N/A'}")
        log("-----------------------------------------------------------")

        # Step 4: Transmit final callback receipt
        payload = {
            "task_id": task_id,
            "status": "ready",
            "relay_a_id": relay_a_id,
            "relay_b_url": relay_b_url,
            "relay_b_id": relay_b_id,
            "relay_c_url": relay_c_url,
            "s3_url": s3_url,
            "s3_key": s3_key_out
        }
        report_telemetry_receipt(callback_url, callback_token, payload)
        log("[DONE] Shard lifecycle cycle completed successfully.")

    except Exception as e:
        log(f"[!] Critical Shard Cycle Failure: {e}")
        try:
            report_telemetry_receipt(callback_url, callback_token, {
                "task_id": task_id,
                "status": "failed",
                "error_message": str(e)
            })
        except:
            pass
        sys.exit(1)

    finally:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                log("[*] Temporary stream buffer cleared.")
            except:
                pass

if __name__ == "__main__":
    main()
