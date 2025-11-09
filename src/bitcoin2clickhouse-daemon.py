#!/usr/bin/env python3

import os
import sys
import time
import signal
import logging
import glob
import argparse
import subprocess
import shutil
import traceback

def format_error_with_location(error, context=""):
    """Format error with file and line number information"""
    
    tb = traceback.extract_tb(error.__traceback__)
    if tb:
        frame = tb[-1]
        filename = frame.filename.split('/')[-1]
        line_number = frame.lineno
        function_name = frame.name
        return f"{context}{error} (File: {filename}, Line: {line_number}, Function: {function_name})"
    else:
        return f"{context}{error}"

def get_last_block_file(blocks_dir):
    """Get the last block file name and modification time."""
    pattern = os.path.join(blocks_dir, "blk*.dat")
    files = glob.glob(pattern)
    if not files:
        return None, None
    
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    last_file = files[0]
    mtime = os.path.getmtime(last_file)
    filename = os.path.basename(last_file)
    return filename, mtime

class Bitcoin2ClickHouseDaemon:
    def __init__(self):
        from dotenv import load_dotenv
        from bitcoin2clickhouse import BitcoinClickHouseLoader
        
        load_dotenv()
        
        self._setup_logging()
        
        params = BitcoinClickHouseLoader.get_connection_params_from_env()
        self.clickhouse_host = params['host']
        self.clickhouse_port = params['port']
        self.clickhouse_user = params['user']
        self.clickhouse_password = params['password']
        self.clickhouse_database = params['database']
        
        self.bitcoin_blocks_dir = os.getenv('BITCOIN_BLOCKS_DIR', '/home/user/.bitcoin/blocks')
        self.xor_dat_path = os.getenv('XOR_DAT_PATH', None)
        
        self.check_period_sec = int(os.getenv('CHECK_PERIOD_SEC', '5'))
        
        self.loader = None
        self.running = True
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _setup_logging(self):
        from bitcoin2clickhouse import setup_logging
        self.logger = logging.getLogger(__name__)
        setup_logging(self.logger, 'daemon.log')
    
    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}. Shutting down gracefully...")
        if self.loader:
            try:
                self.loader.request_stop()
            except Exception:
                pass
        self.running = False
    
    def _validate_environment(self):
        if not os.path.exists(self.bitcoin_blocks_dir):
            self.logger.error(f"Bitcoin blocks directory not found: {self.bitcoin_blocks_dir}")
            return False
        
        if self.xor_dat_path and not os.path.exists(self.xor_dat_path):
            self.logger.error(f"XOR dat file not found: {self.xor_dat_path}")
            return False
        
        return True
    
    def _initialize_loader(self):
        from bitcoin2clickhouse import BitcoinClickHouseLoader
        try:
            self.logger.info("Initializing Bitcoin2ClickHouse loader...")
            params = {
                'host': self.clickhouse_host,
                'port': self.clickhouse_port,
                'user': self.clickhouse_user,
                'password': self.clickhouse_password,
                'database': self.clickhouse_database
            }
            self.loader = BitcoinClickHouseLoader(connection_params=params)
            self.logger.info("Loader initialized successfully")
            return True
        except Exception as e:
            self.logger.error(format_error_with_location(e, "Error initializing loader: "))
            return False
    
    def load_new_blocks(self, blockchain_path, xor_dat_path=None):
        try:
            xor_key = None
            if xor_dat_path:
                with open(xor_dat_path, 'rb') as f:
                    xor_key = f.read()
            
            block_files = sorted(glob.glob(os.path.join(blockchain_path, "blk*.dat")), reverse=True)
            
            start_file = None
            stored_hashes = []
            
            for blk_file_path in block_files:
                stored_hashes = self.loader.daemon_find_stored_blocks(blk_file_path, xor_key)
                if stored_hashes:
                    start_file = blk_file_path
                    break
            
            if start_file is None:
                self.logger.info("No stored blocks found - starting from first file")
                block_files = sorted(glob.glob(os.path.join(blockchain_path, "blk*.dat")))
                if block_files:
                    start_file = block_files[0]
                    stored_hashes = []
                else:
                    self.logger.warning("No block files found")
                    return True
            
            start_index = None
            block_files_sorted = sorted(glob.glob(os.path.join(blockchain_path, "blk*.dat")))
            for i, blk_file_path in enumerate(block_files_sorted):
                if blk_file_path == start_file:
                    start_index = i
                    break
            
            if start_index is None:
                self.logger.error(f"Start file {start_file} not found in sorted list")
                return False

            #if start_index > 0: start_index -= 1
            
            self.loader.daemon_load_new_blocks_from_file(start_file, stored_hashes, xor_key)
            
            for blk_file_path in block_files_sorted[start_index + 1:]:
                self.loader.daemon_load_new_blocks_from_file(blk_file_path, [], xor_key)
            
            return True
            
        except Exception as e:
            self.logger.error(format_error_with_location(e, "Error in load_new_blocks: "))
            raise e
    
    def run(self):
        self.logger.info("Bitcoin2ClickHouse Daemon starting...")
        self.logger.info(f"ClickHouse: {self.clickhouse_host}:{self.clickhouse_port}")
        self.logger.info(f"Database: {self.clickhouse_database}")
        self.logger.info(f"Bitcoin blocks: {self.bitcoin_blocks_dir}")
        if self.xor_dat_path:
            self.logger.info(f"XOR dat file: {self.xor_dat_path}")
        self.logger.info(f"Check period: {self.check_period_sec} seconds")
        self.logger.info("-" * 50)
        
        if not self._validate_environment():
            return 1
        
        if not self._initialize_loader():
            return 1
        
        try:
            self.logger.info("Processing pending blocks before monitoring...")
            self.loader.update_all()
            self.logger.info("Pending blocks processed")
        except Exception as e:
            self.logger.error(format_error_with_location(e, "Error processing pending blocks: "))
        
        self.logger.info("Daemon started. Monitoring for new blocks...")
        self.logger.info("Press Ctrl+C to stop")
        
        last_filename, last_mtime = None, None
        
        while self.running:
            
            try:
                
                current_filename, current_mtime = get_last_block_file(self.bitcoin_blocks_dir)
                
                if current_filename is None:
                    self.logger.warning("No block files found - skipping check")
                    continue
                
                if last_filename != current_filename or last_mtime != current_mtime:
                    
                    try:
                        self.load_new_blocks(self.bitcoin_blocks_dir, self.xor_dat_path)
                        last_filename = current_filename
                        last_mtime = current_mtime
                    except Exception as e:
                        self.logger.error(format_error_with_location(e, "Error in load_new_blocks: "))
                
                time.sleep(self.check_period_sec)
                
                if not self.running:
                    break

            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(format_error_with_location(e, "Unexpected error: "))
                time.sleep(self.check_period_sec)
        
        self.logger.info("Daemon stopped")
        return 0

def get_service_name():
    """Get systemd service name"""
    return "bitcoin2clickhouse"

def get_service_file_path():
    """Get path to systemd service file"""
    return f"/etc/systemd/system/{get_service_name()}.service"

def create_service_file():
    """Create systemd service file content"""
    # Get absolute paths
    script_path = os.path.abspath(__file__)
    
    # Get working directory (project root - go up from src/bitcoin2clickhouse-daemon.py)
    # script_path is in src/, so go up one level
    working_dir = os.path.dirname(os.path.dirname(script_path))
    env_file = os.path.join(working_dir, '.env')
    
    # Try to find Python in venv first, then use system Python
    venv_python = os.path.join(working_dir, 'venv', 'bin', 'python3')
    venv_bin = os.path.join(working_dir, 'venv', 'bin')
    
    # Check if venv Python exists and is executable
    if os.path.exists(venv_python) and os.access(venv_python, os.X_OK):
        python_path = venv_python
        path_env = f"{venv_bin}:/usr/local/bin:/usr/bin:/bin"
    else:
        # Fallback to system Python
        python_path = shutil.which('python3') or sys.executable
        path_env = f"{os.path.dirname(python_path)}:/usr/local/bin:/usr/bin:/bin"
    
    # Get user from environment, SUDO_USER (when running under sudo), or current user
    service_user = os.getenv('SERVICE_USER') or os.getenv('SUDO_USER') or os.getenv('USER', 'root')
    
    # Add PYTHONPATH to include src directory for editable installs
    src_dir = os.path.join(working_dir, 'src')
    pythonpath = f"{src_dir}:{os.environ.get('PYTHONPATH', '')}"
    
    service_content = f"""[Unit]
Description=Bitcoin2ClickHouse Daemon - Bitcoin blockchain parser and ClickHouse loader
After=network.target clickhouse-server.service
Requires=clickhouse-server.service

[Service]
Type=simple
User={service_user}
Group={service_user}
WorkingDirectory={working_dir}
Environment="PATH={path_env}"
Environment="PYTHONPATH={pythonpath}"
"""
    
    if os.path.exists(env_file):
        service_content += f'EnvironmentFile={env_file}\n'
    
    service_content += f"""ExecStart={python_path} {script_path}
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier={get_service_name()}

# Security settings
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
"""
    
    return service_content

def install_service():
    """Install systemd service"""
    if os.geteuid() != 0:
        print("Error: Root privileges required to install systemd service")
        print("Please run: sudo bitcoin2clickhouse-daemon --install")
        return 1
    
    service_file_path = get_service_file_path()
    service_content = create_service_file()
    
    try:
        # Write service file
        with open(service_file_path, 'w') as f:
            f.write(service_content)
        
        print(f"Service file created: {service_file_path}")
        
        # Reload systemd
        subprocess.run(['systemctl', 'daemon-reload'], check=True)
        print("Systemd daemon reloaded")
        
        # Enable service
        subprocess.run(['systemctl', 'enable', get_service_name()], check=True)
        print(f"Service {get_service_name()} enabled")
        
        print(f"\nService installed successfully!")
        print(f"To start the service, run: sudo systemctl start {get_service_name()}")
        print(f"To check status, run: sudo systemctl status {get_service_name()}")
        
        return 0
    except subprocess.CalledProcessError as e:
        print(f"Error executing systemctl command: {e}")
        return 1
    except Exception as e:
        print(f"Error installing service: {e}")
        return 1

def uninstall_service():
    """Uninstall systemd service"""
    if os.geteuid() != 0:
        print("Error: Root privileges required to uninstall systemd service")
        print("Please run: sudo bitcoin2clickhouse-daemon --uninstall")
        return 1
    
    service_file_path = get_service_file_path()
    service_name = get_service_name()
    
    try:
        # Stop service if running
        subprocess.run(['systemctl', 'stop', service_name], check=False)
        
        # Disable service
        subprocess.run(['systemctl', 'disable', service_name], check=False)
        
        # Remove service file
        if os.path.exists(service_file_path):
            os.remove(service_file_path)
            print(f"Service file removed: {service_file_path}")
        
        # Reload systemd
        subprocess.run(['systemctl', 'daemon-reload'], check=True)
        print("Systemd daemon reloaded")
        
        print(f"\nService {service_name} uninstalled successfully!")
        
        return 0
    except subprocess.CalledProcessError as e:
        print(f"Error executing systemctl command: {e}")
        return 1
    except Exception as e:
        print(f"Error uninstalling service: {e}")
        return 1

def main():
    parser = argparse.ArgumentParser(
        description='Bitcoin2ClickHouse Daemon',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Run daemon
  %(prog)s --install          # Install as systemd service (requires root)
  %(prog)s --uninstall        # Uninstall systemd service (requires root)
        """
    )
    
    parser.add_argument(
        '--install',
        action='store_true',
        help='Install as systemd service (requires root privileges)'
    )
    
    parser.add_argument(
        '--uninstall',
        action='store_true',
        help='Uninstall systemd service (requires root privileges)'
    )
    
    args = parser.parse_args()
    
    if args.install:
        return install_service()
    elif args.uninstall:
        return uninstall_service()
    else:
        # Run daemon normally
        daemon = Bitcoin2ClickHouseDaemon()
        return daemon.run()

if __name__ == "__main__":
    sys.exit(main())

