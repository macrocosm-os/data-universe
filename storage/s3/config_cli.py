"""
Command-line tool for managing S3 storage configuration on-chain.
"""
import argparse
import json
import sys
import bittensor as bt
from storage.s3.subnet_config import update_storage_config_on_chain, get_storage_config_from_chain


def main():
    """Main function to handle command-line interface."""
    parser = argparse.ArgumentParser(description="Data Universe S3 Storage Configuration Tool")
    
    # Create subparsers for commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Get command
    get_parser = subparsers.add_parser("get", help="Get current S3 storage configuration")
    get_parser.add_argument("--netuid", type=int, required=True, help="Subnet network UID")
    get_parser.add_argument("--subtensor", type=str, default="finney", help="Subtensor network to connect to")
    
    # Set command
    set_parser = subparsers.add_parser("set", help="Update S3 storage configuration")
    set_parser.add_argument("--netuid", type=int, required=True, help="Subnet network UID")
    set_parser.add_argument("--subtensor", type=str, default="finney", help="Subtensor network to connect to")
    set_parser.add_argument("--wallet.name", type=str, default="default", help="Wallet name")
    set_parser.add_argument("--wallet.hotkey", type=str, default="default", help="Wallet hotkey")
    
    # S3 configuration arguments
    set_parser.add_argument("--use_s3", type=str, choices=["true", "false"], 
                           help="Enable or disable S3 storage")
    set_parser.add_argument("--use_huggingface", type=str, choices=["true", "false"], 
                           help="Enable or disable HuggingFace storage")
    set_parser.add_argument("--bucket_name", type=str, 
                           help="S3 bucket name")
    set_parser.add_argument("--auth_endpoint", type=str, 
                           help="Authentication Lambda endpoint URL")
    set_parser.add_argument("--region", type=str, 
                           help="AWS region for S3 bucket")
    set_parser.add_argument("--rollout_percentage", type=int, 
                           help="Percentage of miners to enable S3 for (0-100)")
    
    # Phased rollout command
    rollout_parser = subparsers.add_parser("rollout", help="Configure phased rollout of S3 storage")
    rollout_parser.add_argument("--netuid", type=int, required=True, help="Subnet network UID")
    rollout_parser.add_argument("--subtensor", type=str, default="finney", help="Subtensor network to connect to")
    rollout_parser.add_argument("--wallet.name", type=str, default="default", help="Wallet name")
    rollout_parser.add_argument("--wallet.hotkey", type=str, default="default", help="Wallet hotkey")
    rollout_parser.add_argument("--percentage", type=int, required=True, 
                              help="Percentage of miners to enable S3 for (0-100)")
    
    # Parse arguments
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return
    
    # Connect to subtensor
    subtensor = bt.subtensor(network=args.subtensor)
    
    if args.command == "get":
        # Get and display current configuration
        config = get_storage_config_from_chain(subtensor, args.netuid)
        if config:
            print(json.dumps(config, indent=2))
        else:
            print("No storage configuration found on-chain")
    
    elif args.command == "set":
        # Load wallet
        wallet = bt.wallet(name=args.wallet_name, hotkey=args.wallet_hotkey)
        
        # Get current configuration
        current_config = get_storage_config_from_chain(subtensor, args.netuid) or {
            "use_s3": False,
            "use_huggingface": True,
            "s3": {},
            "rollout_percentage": 0
        }
        
        # Update with provided arguments
        if args.use_s3 is not None:
            current_config["use_s3"] = args.use_s3.lower() == "true"
        
        if args.use_huggingface is not None:
            current_config["use_huggingface"] = args.use_huggingface.lower() == "true"
        
        # Initialize S3 config if needed
        if "s3" not in current_config:
            current_config["s3"] = {}
        
        if args.bucket_name:
            current_config["s3"]["bucket_name"] = args.bucket_name
        
        if args.auth_endpoint:
            current_config["s3"]["auth_endpoint"] = args.auth_endpoint
        
        if args.region:
            current_config["s3"]["region"] = args.region
            
        if args.rollout_percentage is not None:
            current_config["rollout_percentage"] = max(0, min(100, args.rollout_percentage))
        
        # Update on-chain
        result = update_storage_config_on_chain(subtensor, wallet, args.netuid, current_config)
        
        if result:
            print("Successfully updated storage configuration:")
            print(json.dumps(current_config, indent=2))
        else:
            print("Failed to update storage configuration")
            sys.exit(1)
    
    elif args.command == "rollout":
        # Load wallet
        wallet = bt.wallet(name=args.wallet_name, hotkey=args.wallet_hotkey)
        
        # Get current configuration
        current_config = get_storage_config_from_chain(subtensor, args.netuid) or {
            "use_s3": False,
            "use_huggingface": True,
            "s3": {},
            "rollout_percentage": 0
        }
        
        # Update rollout percentage
        current_config["rollout_percentage"] = max(0, min(100, args.percentage))
        
        # Update on-chain
        result = update_storage_config_on_chain(subtensor, wallet, args.netuid, current_config)
        
        if result:
            print(f"Successfully updated rollout percentage to {args.percentage}%")
        else:
            print("Failed to update rollout percentage")
            sys.exit(1)


if __name__ == "__main__":
    main()