import sys

class Logger:
    def __init__(self, log_file):
        self.log_file = log_file
        try:
            self.log = open(log_file, "a")  
        except IOError as e:
            print(f"Error opening log file {log_file}: {e}")
            sys.exit(1) 

    def write(self, *args, sep=" ", end="\n"):
        try:
           
            message = sep.join(map(str, args)) + end
            self.log.write(message)
            self.log.flush() 
        except IOError as e:
            print(f"Error writing to log file {self.log_file}: {e}")

    def flush(self):
        
        try:
            self.log.flush()
        except IOError as e:
            print(f"Error flushing log file {self.log_file}: {e}")

    def close(self):
       
        try:
            self.log.close()
        except IOError as e:
            print(f"Error closing log file {self.log_file}: {e}")
    
    def __del__(self):
        
        self.close()

    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
