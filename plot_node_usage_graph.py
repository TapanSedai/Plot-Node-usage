from matplotlib import pyplot as plt
import json

# Plot the graph


def get_data(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
    return data

old_data = get_data('panw_current_nodes_before_execution.json')
new_data = get_data('panw_current_nodes_after_execution.json')

# List of [NODE, WORKLOAD_NODE]
nodes_list_old = [old_data[x]["nodegroupdetails"] for x in range(len(old_data))]
nodes_list_new = [new_data[x]["nodegroupdetails"] for x in range(len(new_data))]

nodeGroupList = {}

def get_node_group_details(nodes_list, isBefore):
    for nodeGroup in json.loads(nodes_list):

        nodeGroupName = nodeGroup["nodeGroupName"]
        instanceType = nodeGroup["instanceType"]
        totalMemoryCapacity = nodeGroup["memoryInBytes"] * nodeGroup["numberOfNodes"]
        totalCpuCapacity = nodeGroup["vcpus"] * nodeGroup["numberOfNodes"]

        appsWithResourceDetails = nodeGroup["appsWithResourceDetails"]
        
        memoryOfWorkloads = 0
        cpuOfWorkloads = 0
        for workload in appsWithResourceDetails:
            memoryOfWorkloads += workload["appTotalMemory"]
            cpuOfWorkloads += workload["appTotalCPU"]

        print("Node Group Name: ", nodeGroupName)
        print("Instance Type: ", instanceType)
        print("Total Memory Capacity: ", totalMemoryCapacity)
        print("Memory of Workloads: ", memoryOfWorkloads)    
        print("Total CPU Capacity", totalCpuCapacity)
        print("CPU of Workloads: ", cpuOfWorkloads)
        print("Number of nodes:", nodeGroup["numberOfNodes"])

        if nodeGroupName not in nodeGroupList.keys():
            nodeGroupList[nodeGroupName] = {} 
            nodeGroupList[nodeGroupName]["cpu"] = {}
            nodeGroupList[nodeGroupName]["memory"] = {}

        optimizationTimeConst = "before" if isBefore else "after"
        nodeGroupList[nodeGroupName]["cpu"][optimizationTimeConst] = (cpuOfWorkloads/totalCpuCapacity)*100
        nodeGroupList[nodeGroupName]["memory"][optimizationTimeConst] = (memoryOfWorkloads/totalMemoryCapacity)*100

        print("cpu percentage use", nodeGroupList[nodeGroupName]["cpu"][optimizationTimeConst])
        print("memory percentage use", nodeGroupList[nodeGroupName]["memory"][optimizationTimeConst])

def get_node_only_recommendation():
    print("Node only recommendation")

    print("Data before manual execution")
    get_node_group_details(nodes_list_old[0], True)

    print("Data after manual execution")
    get_node_group_details(nodes_list_new[0], False)

    [print (nodeGroupList[x]["cpu"]) for x in nodeGroupList.keys()]

def plot_results():
    get_node_only_recommendation()
    fig, ax = plt.subplots()
    ax.set_title('Node Group Recommendation')
    ax.set_ylabel('Percentage of Resource Used')
    ax.set_xlabel('Node Group Name')
    ax.set_ylim([0, 100])
    ax.set_xticks(range(len(nodeGroupList)))
    ax.set_xticklabels(list(nodeGroupList.keys()), rotation=20)

    # Define the width of the bars
    bar_width = 0.35

    # Define the position of the bars
    pos_before = range(len(nodeGroupList))
    pos_after = [p + bar_width for p in pos_before]

    #ax.bar(pos_before, [nodeGroupList[x]["cpu"]["before"] for x in nodeGroupList.keys()], bar_width, color='b', label='CPU Before')
    #ax.bar(pos_after, [nodeGroupList[x]["cpu"]["after"] for x in nodeGroupList.keys()], bar_width, color='r', label='CPU After')
    ax.bar(pos_before, [nodeGroupList[x]["memory"]["before"] for x in nodeGroupList.keys()], bar_width, color='g', label='Memory Before')
    ax.bar(pos_after, [nodeGroupList[x]["memory"]["after"] for x in nodeGroupList.keys()], bar_width, color='y', label='Memory After')
    ax.legend()
    plt.show()

plot_results()    


'''
print("Workload Node recommendation")

print("Data before manual execution")
get_node_group_details(nodes_list_old[1], True)

print("Data after manual execution")
get_node_group_details(nodes_list_new[1], False)
'''