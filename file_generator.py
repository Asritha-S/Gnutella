import os
import random
import string

def generate_random_file(file_path, file_size_in_bytes):
  
    with open(file_path, 'w') as file:
        total_written = 0
        while total_written < file_size_in_bytes:
           
            random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=1024))
           
            file.write(random_string + '\n')
            total_written += len(random_string) + 1  
        print(f"File '{file_path}' of size {file_size_in_bytes} bytes generated successfully.")

file_size =1 * 512 * 1024 * 1024   
file_path = "gb.txt"  

generate_random_file(file_path, file_size)
