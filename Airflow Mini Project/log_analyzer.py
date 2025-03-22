import sys
from pathlib import Path

def analyze_file(file_path):
    """Parses a log file to count errors and extract messages."""
    error_count = 0
    error_messages = []
    
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                if "ERROR" in line:
                    error_count += 1
                    error_messages.append(line.strip())
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

    return error_count, error_messages

def analyze_logs(log_dir):
    """Analyzes all log files in the directory and prints results."""
    total_errors = 0
    all_errors = []
    
    log_files = Path(log_dir).rglob("*.log")
    
    for log_file in log_files:
        #print("Accessing log file ", log_file)
        count, errors = analyze_file(log_file)
        total_errors += count
        all_errors.extend(errors)
        #print("------------------------")

    print(f"\nTotal number of errors: {total_errors}\n")
    print("Here are all the errors:\n")
    for error in all_errors:
        print(error)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 log_analyzer.py <log_directory>")
        sys.exit(1)
    
    #log_directory = r'logs'
    log_directory = sys.argv[1]
    analyze_logs(log_directory)
