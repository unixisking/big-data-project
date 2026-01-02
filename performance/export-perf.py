import time
import subprocess
import sys
import matplotlib.pyplot as plt
import numpy as np

# Configuration: Folder names and script types
databases = ['psql', 'mongodb', 'neo4j', 'basex']
tasks = ['data', 'refs']
db_display_names = ['PostgreSQL', 'MongoDB', 'Neo4j', 'BaseX']

# Dictionary to store timing results
results = {db: {'data': 0, 'refs': 0} for db in databases}

print("🚀 Starting Database Performance Benchmark...")

for db in databases:
    for task in tasks:
        script_name = f"export-{task}.py"
        script_path = f"../{db}/{script_name}"
        
        print(f"⏱️  Timing {script_path}...")
        
        start_time = time.perf_counter()
        try:
            # Runs the script using the current Python interpreter
            subprocess.run([sys.executable, script_path], check=True, capture_output=True)
            end_time = time.perf_counter()
            
            duration = end_time - start_time
            results[db][task] = round(duration, 2)
            print(f"✅ Finished {db} {task} in {duration:.2f} seconds.")
        except Exception as e:
            print(f"❌ Error running {script_path}: {e}")
            results[db][task] = 0

# Data for Plotting
data_times = [results[db]['data'] for db in databases]
refs_times = [results[db]['refs'] for db in databases]

x = np.arange(len(db_display_names))
width = 0.35

# Generate the Matplotlib Graph
fig, ax = plt.subplots(figsize=(10, 6))
rects1 = ax.bar(x - width/2, data_times, width, label='Data Export (Result 1)', color='#3498db')
rects2 = ax.bar(x + width/2, refs_times, width, label='Refs Export (Result 2)', color='#e67e22')

# Add labels and titles
ax.set_ylabel('Execution Time (seconds)')
ax.set_title('Database Export Performance Comparison (9.3GB Dataset)')
ax.set_xticks(x)
ax.set_xticklabels(db_display_names)
ax.legend()

# Function to add values on top of bars
def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f'{height}s',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontweight='bold')

autolabel(rects1)
autolabel(rects2)

plt.tight_layout()
plt.savefig('db_performance_comparison.png')
print("\n📊 Benchmark complete! Graph saved as 'db_performance_comparison.png'.")
plt.show()