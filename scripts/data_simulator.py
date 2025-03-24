import json
import random
import csv
import argparse
from datetime import datetime, timedelta
from tqdm import tqdm
import numpy as np

def generate_device_metrics(device_id, device_type, start_time, rows):
    metrics_list = []
    current_time = start_time
    uptime = random.randint(20000, 50000)  # Start with random uptime
    cpu_drift = 0
    mem_drift = 0
    drift_steps = 0
    flatline_cpu = None
    flatline_mem = None
    flatline_counter = 0

    for _ in range(rows):
        # Time increment
        current_time += timedelta(seconds=30)
        uptime += 30

        # Reboot event (1% chance every 1000 rows)
        if random.random() < 0.001:
            uptime = random.randint(1000, 5000)
            # Also spike CPU during reboot
            cpu_usage = min(100, random.randint(80, 100))
        else:
            # CPU drift: gradual increases
            if drift_steps > 0:
                cpu_drift += 0.3
                drift_steps -= 1
            elif random.random() < 0.002:
                drift_steps = random.randint(50, 150)
                cpu_drift = 0
            
            # CPU spikes (0.5% chance)
            if random.random() < 0.005:
                cpu_usage = min(100, random.randint(80, 100))
            elif flatline_cpu is not None and flatline_counter > 0:
                cpu_usage = flatline_cpu
                flatline_counter -= 1
            elif random.random() < 0.003:
                flatline_cpu = random.randint(20, 50)
                flatline_counter = random.randint(10, 20)
                cpu_usage = flatline_cpu
            else:
                cpu_usage = min(100, max(5, 30 + np.random.normal(0, 5) + cpu_drift))

        # Memory drift & flatline similar
        if drift_steps > 0:
            mem_drift += 0.2
        if random.random() < 0.005:
            memory_usage = min(100, random.randint(80, 95))
        elif flatline_mem is not None and flatline_counter > 0:
            memory_usage = flatline_mem
        elif random.random() < 0.003:
            flatline_mem = random.randint(40, 60)
            flatline_counter = random.randint(10, 20)
            memory_usage = flatline_mem
        else:
            memory_usage = min(100, max(10, 50 + np.random.normal(0, 7) + mem_drift))

        inbound_traffic = max(1000, int(np.random.normal(200000, 50000)))
        outbound_traffic = max(1000, int(np.random.normal(200000, 50000)))

        input_errors = random.choices([0, 1, 2, 10], weights=[90, 5, 4, 1])[0]
        output_errors = random.choices([0, 1, 2, 5], weights=[92, 5, 2, 1])[0]

        # Random missing values (5% chance)
        if random.random() < 0.05:
            cpu_usage = None if random.random() < 0.5 else cpu_usage
            memory_usage = None if random.random() < 0.3 else memory_usage
            inbound_traffic = None if random.random() < 0.2 else inbound_traffic
            outbound_traffic = None if random.random() < 0.2 else outbound_traffic

        metrics_list.append({
            "timestamp": current_time,
            "device_id": device_id,
            "device_type": device_type,
            "system_uptime": uptime,
            "cpu_usage": cpu_usage,
            "memory_usage": memory_usage,
            "inbound_traffic": inbound_traffic,
            "outbound_traffic": outbound_traffic,
            "input_errors": input_errors,
            "output_errors": output_errors
        })

    return metrics_list
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Path to devices_config.json')
    parser.add_argument('--rows', type=int, required=True, help='Number of rows per device')
    parser.add_argument('--output', required=True, help='Output CSV file name')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        devices_data = json.load(f)

    # If the devices data is a list, assign directly; if dict with 'devices' key, handle accordingly
    devices = devices_data if isinstance(devices_data, list) else devices_data.get("devices", [])

    all_data = []
    start_time = datetime.now() - timedelta(days=1)  # simulate past 24 hrs

    print(f"Generating data for {len(devices)} devices...")
    for device in tqdm(devices):
        if isinstance(device, dict):
            device_metrics = generate_device_metrics(
                device["device_id"], device["device_type"], start_time, args.rows
            )
            all_data.extend(device_metrics)

    fieldnames = ["timestamp", "device_id", "device_type", "system_uptime", "cpu_usage", "memory_usage", "inbound_traffic", "outbound_traffic", "input_errors", "output_errors"]

    with open(args.output, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_data)

    print(f"\nGenerated {len(all_data)} rows of data and saved to {args.output}")

if __name__ == "__main__":
    main()
