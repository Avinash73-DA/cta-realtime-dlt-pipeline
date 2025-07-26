import requests
from time import sleep
from zoneinfo import ZoneInfo
from datetime import datetime

bus_key = 'C5VvgrteMGMwFBzrpGZzZeDr9'
landing_path = '/Volumes/cta_project/landing_volumes/buses/'  # Bronze Layer

def get_all_bus_routes(api_key):
    routes_url = f'http://www.ctabustracker.com/bustime/api/v2/getroutes?key={api_key}&format=json'
    response = requests.get(routes_url)
    response.raise_for_status()
    routes = response.json()['bustime-response']['routes']
    return [route['rt'] for route in routes]

try:
    all_routes = get_all_bus_routes(bus_key)
    chunk_size = 10
    all_raw_chunks = []

    print(f"🚚 Fetching vehicle data in {len(all_routes) // chunk_size + 1} chunks...")

    for i in range(0, len(all_routes), chunk_size):
        route_chunk = all_routes[i:i + chunk_size]
        try:
            vehicles_url = f"http://www.ctabustracker.com/bustime/api/v2/getvehicles?key={bus_key}&rt={','.join(route_chunk)}&format=json"
            response = requests.get(vehicles_url)
            response.raise_for_status()

            raw_json = response.text  
            all_raw_chunks.append(raw_json)

        except requests.exceptions.RequestException as req_e:
            print(f" Failed to fetch data for chunk {i // chunk_size}. Error: {repr(req_e)}")
            continue
        except Exception as e:
            print(f" Unexpected error in chunk {i // chunk_size}: {repr(e)}")
            continue

        sleep(1)  #rate limits

    if all_raw_chunks:
        timestamp = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d_%H_%M_%S")
        file_name = f"all_bus_positions_raw_{timestamp}.json"
        full_file_path = f"{landing_path}{file_name}"

        with open(full_file_path, 'w') as file:
            file.write("[\n" + ",\n".join(all_raw_chunks) + "\n]") 

        print(f"\n Saved raw API responses from all chunks to: {file_name}")
    else:
        print("\n⚠ No vehicle data collected. No file saved.")

except Exception as e:
    print(f"\n Unrecoverable error: {repr(e)}")
