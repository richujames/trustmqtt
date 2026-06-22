import argparse
import threading
import time
from devices.temperature_sensor import TemperatureSensor
from devices.door_lock import DoorLock
from devices.motion_sensor import MotionSensor
from attacks.credential_replay_attack import CredentialReplayAttack
from attacks.recon_wildcard_subscribe import ReconWildcardAttack
from attacks.different_client_lib_attack import DifferentClientLibAttack
from attacks.gradual_drift_firmware_sim import GradualDriftAttack

def main():
    parser = argparse.ArgumentParser(description="TrustMQTT Traffic Simulator")
    parser.add_argument("--duration", type=int, default=60, help="Duration to run simulation (seconds)")
    parser.add_argument("--attack", type=str, choices=['replay', 'recon', 'lib', 'drift'], help="Optional attack to launch")
    args = parser.parse_args()

    # Define our baseline normal fleet
    devices = [
        TemperatureSensor("temp_sensor_01"),
        TemperatureSensor("temp_sensor_02"),
        DoorLock("door_lock_01"),
        MotionSensor("motion_sensor_01")
    ]

    threads = []
    print("[*] Starting normal devices to establish baseline...")
    for dev in devices:
        t = threading.Thread(target=dev.run)
        t.daemon = True
        t.start()
        threads.append((t, dev))

    attack_thread = None
    attack_dev = None
    if args.attack:
        print(f"[*] Waiting 15s for baseline to establish before launching attack: {args.attack}")
        time.sleep(15)
        
        if args.attack == 'replay':
            # Attacker steals temp_sensor_01's identity
            attack_dev = CredentialReplayAttack("temp_sensor_01")
        elif args.attack == 'recon':
            # Attacker compromises temp_sensor_02 and starts scanning
            attack_dev = ReconWildcardAttack("temp_sensor_02")
        elif args.attack == 'lib':
            # Attacker uses generic python script to mimic door lock
            attack_dev = DifferentClientLibAttack("door_lock_01")
        elif args.attack == 'drift':
            # Firmware drift on temp_sensor_02
            attack_dev = GradualDriftAttack("temp_sensor_02")
            
        if attack_dev:
            attack_thread = threading.Thread(target=attack_dev.run)
            attack_thread.daemon = True
            attack_thread.start()

    print(f"[*] Simulation running for {args.duration} seconds...")
    time.sleep(args.duration)

    print("[*] Stopping all devices...")
    for t, dev in threads:
        dev.disconnect()
        
    if attack_dev:
        attack_dev.disconnect()

    print("[*] Simulation complete.")

if __name__ == "__main__":
    main()
