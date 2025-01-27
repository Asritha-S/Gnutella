import asyncio
import math

low_peers = {}
med_peers = {}
high_peers = {}

num_connections = {}

MIN_CONNS = 2

def disconnect_the_peer(my_port):
    if my_port in low_peers:
        del low_peers[my_port]
    elif my_port in med_peers:
        del med_peers[my_port]
    elif my_port in high_peers:
        del high_peers[my_port]
    if my_port in num_connections:
        del num_connections[my_port]
    print(f"Updated active peers: Low={len(low_peers)}, Med={len(med_peers)}, High={len(high_peers)}")



async def handle_client(reader, writer):
    peer_addr = writer.get_extra_info('peername')
    print(f"Peer connected: {peer_addr}")

    try:
        
        data = await reader.readline()
        data = data.decode().strip()
        print(f"Recvd msg in server : {data}")
        
        msg, bandwidth, cpu, ram, my_port = map(int, data.split(':'))
        if(msg==0):
            disconnect_the_peer(my_port)
            writer.write(" Diconnected Succesfully ".encode() + b'\n')
        else:
           
            magnitude = math.sqrt(bandwidth**2 + cpu**2 + ram**2)
            print(f"Received data from {peer_addr}: my_port={my_port}, "
                f"bandwidth={bandwidth}, cpu={cpu}, ram={ram}, magnitude={magnitude}")

            peer_data = {
                "bandwidth": bandwidth,
                "cpu": cpu,
                "ram": ram,
            }

           
            if magnitude < 2:
                curr_class = "low"
                low_peers[my_port] = peer_data
                eligible_peers = set(med_peers.keys())
            elif 2 <= magnitude < 4:
                curr_class = "medium"
                med_peers[my_port] = peer_data
                eligible_peers = set(high_peers.keys())
            else:
                curr_class = "high"
                high_peers[my_port] = peer_data
                eligible_peers = set(high_peers.keys()) - {my_port}

            print(f"Current class: {curr_class}")
            print(f"Eligible peers: {eligible_peers}")

            num_connections[my_port] = 0  

          
            peers_to_send = sorted(
                ((key, value) for key, value in num_connections.items() if key in eligible_peers),
                key=lambda item: item[1]  
            )
            print("After sorting ", peers_to_send)
           
            known_peers = [str(key) for key, _ in peers_to_send][:MIN_CONNS]

            if len(known_peers) < MIN_CONNS:
                print(f"Not enough peers in the eligible class. Adding from closest class...")

                if curr_class == "low":
                    additional_peers = sorted(
                        ((key, value) for key, value in num_connections.items() if key in low_peers and key!=my_port),
                        key=lambda item: item[1]
                    )
                elif curr_class == "medium":
                    additional_peers = sorted(
                        ((key, value) for key, value in num_connections.items() if key in med_peers and key!=my_port),
                        key=lambda item: item[1]
                    )
                else:  
                    additional_peers = sorted(
                        ((key, value) for key, value in num_connections.items() if key in med_peers and key!= my_port),
                        key=lambda item: item[1]
                    )

                for key, _ in additional_peers:
                    if len(known_peers) >= MIN_CONNS:
                        break
                    if str(key) not in known_peers:
                        known_peers.append(str(key))
            for key in known_peers:
                num_connections[int(key)] = num_connections.get(int(key), 0) + 1
            print(f"Updated active peers: Low={len(low_peers)}, Med={len(med_peers)}, High={len(high_peers)}")
            print(f"Sending peers to {my_port}: {known_peers}")

            
            if known_peers:
                writer.write(':'.join(known_peers).encode())
            else:
                writer.write(b"\n")  
            await writer.drain()

    except ValueError:
       
        print(f"Invalid data received from {peer_addr}")
        writer.write(b"Invalid data format\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        return

    except Exception as e:
        
        print(f"An error occurred with peer {peer_addr}: {e}")
        writer.close()
        await writer.wait_closed()
        return

   
    await asyncio.sleep(1)
    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_client, '127.0.0.1', 9000)
    print("Bootstrap server running on 127.0.0.1:9000")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
