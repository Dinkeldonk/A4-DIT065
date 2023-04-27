import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style('whitegrid')
#import re

def open_file(filename):
    with open(filename, "r") as infile:
        return infile.read()

def read_filelines(filename):
    with open(filename, "r") as infile:
        return infile.readlines()
    
def text_to_list(text):
    """
    Splits the text file to only keep the lists
    """
    text = text.split("#### ")
    text = text[1]
    speedup = []
    theoretical = []
    nr_cores = [] 
    text = text.replace("[", "")
    text = text.replace("]", "")
    text = text.split(":")
    speed = text[1]
    speed = speed.split(",")
    #amdahl = text[3]
    #amdahl = amdahl.split(",")
    cores = text[5]
    cores = cores.split(",")

    for i in speed:
        speedup.append(float(i))
    #for j in amdahl:
    #    theoretical.append(float(j))
    for k in cores:
        nr_cores.append(int(k))

    return speedup, nr_cores #, theoretical 

def get_cores_and_times(text):
    """ cores = []
        times = []
        for line in text:
            if line.startswith('Number'):
                num_cores = line.split(' ')[-1]
                cores.append(int(num_cores))
            elif line.startswith('Total running'):
                time = line.split(' ')[-1]
                times.append(float(time))"""
    cores_times = []
    num_xprmnts_per_data_size = 6
    experiments = []
    for i in range(0, len(text), num_xprmnts_per_data_size):
        experiments.append(text[i : i+num_xprmnts_per_data_size])
    print(experiments)
    for experiment in experiments:
        exp_res = []
        for line in experiment:
            line = line.split()
            num_cores = int(line[0])
            time = float(line[1])
            exp_res.append((num_cores, time))
        print('exp_res:', exp_res)
        exp_res = sorted(exp_res)
        print('sorted exp_res:', exp_res)
        cores_times.append(exp_res)
    return cores_times


def read_mrjob(filename):
    with open(filename) as f:
        return f.readlines()

def get_times(text_lines):
    line = text_lines[-1]
    line = line.replace("[", "")
    line = line.replace("]", "")
    times = line.split(',')
    for i, time in enumerate(times):
        time = float(time.strip())
        times[i] = time
    return times

#text = "slurm-11198.out"
filename = 'prob2b_results'
#filename = '100_rows.out'

# out = open_file(text)
text = read_filelines(filename)
#text = read_mrjob(filename)

#speed, cores = text_to_list(out)
cores_times = get_cores_and_times(text)
#times = get_times(text)
#cores = [1,2,4,8,16,32]

print(cores_times)

experiments = []
for i in range(len(cores_times)):
    cores, times = zip(*cores_times[i])
    speedups = []
    for time in times:
        speedups.append(times[0] / time)
    experiments.append(speedups)
    plt.plot(cores, speedups, )


print(cores)
print(times)

# speedups = []
# for time in times:
#     speedups.append(times[0] / time)

save_filename = 'p2b_speedup_plot.png'

#plt.plot(cores, speedups)
#plt.plot(cores, theo)
plt.title("Actual speedup over number of cores")
plt.xlabel("n_cores")
plt.xticks(cores)
plt.ylabel("speedup")
plt.legend(['10M','100M','1B'])
plt.savefig(save_filename)
plt.show()
