import asyncio
import os
import socket

class File_Server:
    def __init__(self, logger, host='localhost', directory='shared_files'):
        self.logger = logger
        self.host = host
        self.port = self.get_random_port()
        self.directory = directory
        self.server = None  

        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def get_random_port(self):
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, 0))  
            return s.getsockname()[1]  

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        self.logger.write(f"Connection from {addr} has been established.")
        
        
        request_msg = await reader.read(1024)  
        request_msg = request_msg.decode().strip()  
        
        
        file_name, chunk_start, chunk_end = request_msg.split(',')
        chunk_start, chunk_end = int(chunk_start), int(chunk_end)
        
       
        full_path = os.path.join(self.directory, file_name)
        
        self.logger.write(f"Client requested file: {file_name} from byte {chunk_start} to {chunk_end}")
        
        if os.path.exists(full_path) and os.path.isfile(full_path):
           
            await self.send_chunk(writer, full_path, chunk_start, chunk_end, 45*1024)
        else:
            writer.write(f"File '{file_name}' not found in directory '{self.directory}'.\n".encode())
            await writer.drain()
        
        self.logger.write(f"Closing connection with {addr}.")
        writer.close()
        await writer.wait_closed()

    async def send_chunk(self, writer, filepath, chunk_start, chunk_end, chunk_size=1024):
        
        with open(filepath, 'rb') as f:
            f.seek(chunk_start)
            offset = chunk_start
           
            while offset < chunk_end:
                rem_chunk = chunk_end - offset
                curr_chunk_size = min(chunk_size, rem_chunk) 
                data = f.read(curr_chunk_size)
                if data:
                    writer.write(data)  
                    await writer.drain()  
                    self.logger.write(f"Sent {len(data)} bytes of {os.path.basename(filepath)} from byte {offset} to {offset + len(data)}.")
                    offset += len(data)  
                    await asyncio.sleep(0.02)
                else:
                    self.logger.write(f"No data to send for the chunk from {offset} to {chunk_end}.")
                    break  

    async def start_server(self):
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port)
        
        self.logger.write(f"File Sharing Server started on port: {self.port}")

        async with self.server:
            await self.server.serve_forever()

    async def stop_server(self):
        """Stop the file server gracefully."""
        if self.server:
            self.logger.write(f"Stopping File Sharing Server on port: {self.port}")
            self.server.close()
            await self.server.wait_closed()
            self.logger.write("File Sharing Server stopped.")
        else:
            self.logger.write("Server is not running, cannot stop.")

if __name__ == "__main__":
   
    file_sharing_server = File_Server(directory='shared_files')
    
    
    try:
        asyncio.run(file_sharing_server.start_server())
    except KeyboardInterrupt:
        
        asyncio.run(file_sharing_server.stop_server())
