import asyncio
import sys
import os
import time
from gnutella_peer import GnutellaPeer
from logger import Logger
import aioconsole

HOST = "127.0.0.1"
SERVER_PORT=9000

async def connect_to_server(msg, my_port, bandwidth, cpu, ram):
    try:
        reader, writer = await asyncio.open_connection(HOST, SERVER_PORT)
        print(f"Connected to bootstrap server at {HOST}:{SERVER_PORT}")

       
        msg_code=1 
        if(msg=="exit"):
            msg_code=0
        print(f"Sending data to server: {msg_code}:{bandwidth}:{cpu}:{ram}:{my_port}")
        writer.write(f"{msg_code}:{bandwidth}:{cpu}:{ram}:{my_port}\n".encode())
        await writer.drain()

        
        data = await reader.read(4096)
        writer.close()
        await writer.wait_closed()
        if msg=="connect":
            peers_list = data.decode().split(':')

            if not peers_list:
                print("No peers received from the bootstrap server.")
                return []

            print(f"Received list of peers from the bootstrap server: {peers_list}")
            return [int(peer) for peer in peers_list]
        else:
            print(f"Received message: {data}")

    except ConnectionRefusedError:
        print(f"Connection to {HOST}:{SERVER_PORT} failed")
        return []
    except Exception as e:
        print(f"An error occurred while connecting to the server: {e}")
        return []



def setup_directory(directory):
    if directory:
        if not os.path.isdir(directory):
            os.makedirs(directory)
        log_file_path = os.path.join(directory, "output.log")
        with open(log_file_path, 'w'):
            pass 



async def handle_user_input(peer):
    while True:
        file_name = await aioconsole.ainput("Enter the file name you want to download (or type 'exit' to quit): ")
        if file_name.lower() == 'exit':
            print("Exiting...")
            
            await connect_to_server("exit", peer.port, 0, 0, 0)
            await peer.disconnect_from_peers() 
            await peer.stop_server()
            await peer.file_server.stop_server()
            print("Shutting down...")
            break

        try:
            start_time = time.monotonic()
            response = await peer.search_for_file(file_name)
            end_time = time.monotonic()
            total_time = end_time - start_time
            print(f"Request completed in {total_time:.2f} seconds.")
            print(f"Response: {response}")
        except Exception as e:
            print(f"Error during file search: {e}")



async def run_task(task, name):
    try:
        await task
    except Exception as e:
        print(f"Error in {name}: {e}")


async def monitor_tasks():
    while True:
        print(f"Active tasks: {len(asyncio.all_tasks())}")
        await asyncio.sleep(5)


async def main():
    args = sys.argv[1:]

    target_port = None
    directory = None
    bandwidth = None
    cpu = None
    ram = None
   
    has_bandwidth = False
    has_cpu = False
    has_ram = False
    has_port = False

    for arg in args:
        if arg == "-b":
            has_bandwidth = True
        elif has_bandwidth:
            bandwidth = int(arg)
            has_bandwidth = False
        elif arg == "-c":
            has_cpu = True
        elif has_cpu:
            cpu = int(arg)
            has_cpu = False
        elif arg == "-r":
            has_ram = True
        elif has_ram:
            ram = int(arg)
            has_ram = False
        elif arg == "-p":
            has_port = True
        elif has_port:
            target_port = int(arg)
            has_port = False
        else:
            directory = arg

    setup_directory(directory)

    port = target_port if target_port else 12345
    peer = GnutellaPeer(port, bandwidth, directory)

    
    asyncio.create_task(run_task(peer.start_server(), "start_server"))
    asyncio.create_task(run_task(peer.start_file_server(), "start_file_server"))


    
    available_peers = await connect_to_server("connect", port, bandwidth, cpu, ram)
    if available_peers:
        print("Available peers:", available_peers)
        await peer.connect_to_initial_peers(available_peers)
        peer.print_connected_peers() 
   
    else:
        print("No available peers found.")

    
    asyncio.create_task(handle_user_input(peer))
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
