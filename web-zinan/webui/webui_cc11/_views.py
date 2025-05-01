from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader
from django.views.decorators.csrf import csrf_exempt
import subprocess

def stop_and_remove_containers(image_name):
    try:
        # Step 1: List containers
        list_containers_cmd = [
            "sudo", "docker", "ps", "-q",
            "--filter", f"ancestor={image_name}",
            "--format", "{{.ID}}"
        ]
        containers = subprocess.run(list_containers_cmd, capture_output=True, text=True, check=True)
        container_ids = containers.stdout.strip()

        if container_ids:
            # Step 2: Stop containers
            stop_containers_cmd = ["sudo", "docker", "stop"] + container_ids.split()
            subprocess.run(stop_containers_cmd, check=True)

            # Step 3: Remove containers
            remove_containers_cmd = ["sudo", "docker", "rm"] + container_ids.split()
            subprocess.run(remove_containers_cmd, check=True)

            print("Containers stopped and removed successfully.")
        else:
            print("No containers to stop or remove.")

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")

@csrf_exempt
# Create your views here.
def members(request):
    template=loader.get_template('webui.html')
    return HttpResponse(template.render())
@csrf_exempt
def process_query(request):
    # Retrieve the query from the request
    query = request.POST.get('query', 'No topic entered')

    # Print the query to the console
    print(f"Received query: {query}")
    stop_and_remove_containers("reddit-producer-shihan")
    command = (
    f"sudo docker run --network=host "
    f"-e 'BOOTSTRAP_SERVERS=129.114.27.101:30000' "
    f"-e 'FILTER_NAME={query}' "
    "reddit-producer-shihan"
    )
    result=subprocess.run(command,shell=True,capture_output=True,text=True) 
    #print("stdout:", result.stdout)
    print("stderr:", result.stderr)

    # You could do processing here and then return a response
    return HttpResponse(f"Processing: {query}")