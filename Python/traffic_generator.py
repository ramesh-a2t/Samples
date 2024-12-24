import os
import random
import uuid
import json
import numpy as np
import cv2
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# Constants for directories
VEHICLE_IMAGES_DIR = ""
OUTPUT_IMAGES_DIR = r"\\WIN02\TollHost\vehicle_images"
TRXDATA_DIR = r"\\WIN02\TollHost\trxdata"
os.makedirs(OUTPUT_IMAGES_DIR, exist_ok=True)
os.makedirs(TRXDATA_DIR, exist_ok=True)

# Azure Service Bus Configuration
USE_AZURE_SERVICE_BUS = False  # Set to True to enable Azure Service Bus integration
SERVICE_BUS_CONNECTION_STRING = ""
QUEUE_NAME = "samplequeue"

# Plate states and formats
PLATE_STATES = {
    "Illinois": {"full_name" : "Illinois", "abbreviation": "IL", "probability": 0.1, "format": "A######", "plate_types": ["Passenger", "Commercial", "Firefighter", "Doctor"]},
    "Pennsylvania": {"full_name" : "Pennsylvania", "abbreviation": "PA", "probability": 0.6, "format": "AAA####"},
    "New Jersey": {"full_name" : "New Jersey", "abbreviation": "NJ", "probability": 0.1, "format": "A##AAA"},
    "New York": {"full_name" : "New York", "abbreviation": "NY", "probability": 0.05, "format": "AAA####"},
    "Maryland": {"full_name" : "Maryland", "abbreviation": "MD", "probability": 0.05, "format": "AAA###"},
    "Ohio": {"full_name" : "Ohio", "abbreviation": "OH", "probability": 0.1, "format": "AAA####"},
}

FONT_PATH = "arialbd.ttf"  # Update as needed
PLATE_FONT_SIZE = 24
STATE_FONT_SIZE = 9
PLATE_PADDING = 5

# Helper functions
def random_date(start, end):
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

def random_plate_number(format_string):
    plate_number = ""
    for char in format_string:
        if char == "A":
            plate_number += random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        elif char == "#":
            plate_number += random.choice("0123456789")
    return plate_number

def random_plate_state():
    state = random.choices(
        list(PLATE_STATES.keys()),
        weights=[PLATE_STATES[state]["probability"] for state in PLATE_STATES],
        k=1
    )[0]
    return PLATE_STATES[state]  # Return the full dictionary

def apply_effects(image, effect):
    # Convert Pillow image to a NumPy array (grayscale for infrared)
    img_cv = np.array(image)

    # Ensure the image is single-channel grayscale
    if len(img_cv.shape) == 3 and img_cv.shape[2] == 3:  # If it's RGB, convert to grayscale
        img_cv = cv2.cvtColor(img_cv, cv2.COLOR_RGB2GRAY)

    if effect == 'blurry':
        # Apply Gaussian blur for blurry effect
        img_cv = cv2.GaussianBlur(img_cv, (7, 7), 2)
    elif effect == 'dirty':
        # Add random noise for dirty effect
        noise = np.random.randint(0, 50, img_cv.shape, dtype='uint8')
        img_cv = cv2.add(img_cv, noise)
    elif effect == 'rainy':
        # Simulate rain with semi-transparent streaks
        img_cv_color = cv2.cvtColor(img_cv, cv2.COLOR_GRAY2RGB)  # Convert grayscale to RGB
        img_pil = Image.fromarray(img_cv_color)
        draw = ImageDraw.Draw(img_pil)
        for _ in range(50):  # Add rain streaks
            x1, y1 = random.randint(0, img_pil.width), random.randint(0, img_pil.height)
            x2, y2 = x1 + random.randint(-10, 10), y1 + random.randint(20, 50)
            draw.line((x1, y1, x2, y2), fill="white", width=1)
        img_cv = np.array(img_pil.convert('L'))  # Convert back to grayscale
    elif effect == 'snowy':
        # Simulate snow with random white dots
        snow = np.random.randint(0, 255, img_cv.shape, dtype='uint8')
        snow = cv2.threshold(snow, 250, 255, cv2.THRESH_BINARY)[1]  # Create white dots
        img_cv = cv2.addWeighted(img_cv, 0.9, snow, 0.1, 0)

    # Convert back to a Pillow image
    return Image.fromarray(img_cv)

# Overlay license plate and state
def overlay_plate_info(image, plate_area, plate_number, plate_state, plate_type):
    draw = ImageDraw.Draw(image)
    draw.rectangle(plate_area, fill=(200, 200, 200), outline="gray")
    
    # Add state name
    state_font = ImageFont.truetype(FONT_PATH, STATE_FONT_SIZE)
    state_x = plate_area[0] + (plate_area[2] - plate_area[0] - draw.textlength(plate_state["full_name"], font=state_font)) // 2
    state_y = plate_area[1] + PLATE_PADDING
    draw.text((state_x, state_y), plate_state["full_name"], fill="black", font=state_font)
    
    # Add plate number
    plate_font = ImageFont.truetype(FONT_PATH, PLATE_FONT_SIZE)
    plate_x = plate_area[0] + (plate_area[2] - plate_area[0] - draw.textlength(plate_number, font=plate_font)) // 2
    plate_y = state_y + 10
    draw.text((plate_x, plate_y), plate_number, fill="black", font=plate_font)

    # Add plate type at the bottom center of the plate area (if provided)
    if plate_type:
        plate_type_font = ImageFont.truetype(FONT_PATH, STATE_FONT_SIZE)
        plate_type_x = plate_area[0] + (plate_area[2] - plate_area[0] - draw.textlength(plate_type, font=plate_type_font)) // 2
        plate_type_y = plate_area[3] - 12  # At the bottom of the plate area
        draw.text((plate_type_x, plate_type_y), plate_type, fill="black", font=plate_type_font)

def generate_vehicle_images(transaction_id, plate_number, plate_state, plate_type=None, vehicle_type_distribution=None, max_vehicle_images=1):
    """
    Generate images of vehicles with license plates.
    Includes overview images and a cropped license plate image.
    """
    if vehicle_type_distribution is None:
        vehicle_type_distribution = {
            "Car": 0.6,
            "SUV": 0.2,
            "Small Truck": 0.1,
            "Tractor Trailer": 0.1
        }

    # Randomly determine the vehicle type
    vehicle_type = random.choices(
        list(vehicle_type_distribution.keys()),
        weights=vehicle_type_distribution.values(),
        k=1
    )[0]

    # File paths for images
    images = []
    effect = random.choices(["clear", "dirty", "blurry", "snow", "rain"], weights=[0.83, 0.02, 0.05, 0.05, 0.05], k=1)[0]

    # Create overview images (2 or 3)
    for i in range(1, random.randint(2, 4)):
        vehicle_image_file = f"{transaction_id}_vehicle_{i}.jpg"
        vehicle_image_path = os.path.join(OUTPUT_IMAGES_DIR, vehicle_image_file)

        # Create an overview image
        vehicle_image_path = os.path.join(VEHICLE_IMAGES_DIR, "vehicle.jpg")  # Replace with actual image
        img = Image.open(vehicle_image_path).convert("RGB")

        # Simulate a vehicle and license plate area
        plate_area = (250, 450, 450, 500)  # Adjust to match the image
        overlay_plate_info(img, plate_area, plate_number, plate_state, plate_type)

        # Apply random effect
        img = apply_effects(img, effect)
        
        # Generate multiple vehicle images (1 to max_vehicle_images)
        for i in range(1, max_vehicle_images):
            vehicle_image_file = f"{transaction_id}_vehicle_{i}.jpg"
            vehicle_image_path = os.path.join(OUTPUT_IMAGES_DIR, vehicle_image_file)
            img.save(vehicle_image_path)
            images.append({"image_file": vehicle_image_path, "image_type": "1"}) # image_type 1 = Rear Overview, 2 = Region of Interest (ROI)


    # Create cropped license plate image (400x100)
    cropped_plate = img.crop(plate_area)
    cropped_plate = cropped_plate.resize((400, 100), Image.Resampling.LANCZOS)
    cropped_plate_file = f"{transaction_id}_plateroi.jpg"
    cropped_plate_path = os.path.join(OUTPUT_IMAGES_DIR, cropped_plate_file)
    cropped_plate.save(cropped_plate_path)
    images.append({"image_file": cropped_plate_path, "image_type": "2"})

    return images

# Send JSON data to Azure Service Bus
def send_to_azure_service_bus(batch):
    with ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STRING) as client:
        with client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
            for record in batch:
                message = ServiceBusMessage(json.dumps(record))
                sender.send_messages(message)
    print(f"Batch of {len(batch)} sent to Azure Service Bus queue: {QUEUE_NAME}")

def generate_traffic_data(start_date, end_date, average_daily_volume, batch_size=10):
    total_days = (end_date - start_date).days
    total_records = average_daily_volume * total_days
    batch = []
    record_count = 0

    for _ in range(total_records):
        transaction_id = str(uuid.uuid4())
        plate_state_info = random_plate_state()  # Returns the full dictionary
        plate_number = random_plate_number(plate_state_info["format"])

        # Generate plate types for Illinois only
        plate_types = None
        if plate_state_info["abbreviation"] == "IL":
            plate_types = random.choice(plate_state_info.get("plate_types", []))
        
        image_count = random.randint(1, 3)
        # Generate vehicle images
        vehicle_images = generate_vehicle_images(
            transaction_id,
            plate_number,
            plate_state=plate_state_info,  # Pass the full state dictionary
            plate_type=plate_types,
            max_vehicle_images=image_count
        )

        # Create record
        record = {
            "LaneTransId": transaction_id,
            "TransactionDt": random_date(start_date, end_date).isoformat(),
            "LocationId": random.randint(1001, 1008),
            "LocationTypeId": random.randint(1, 5),
            "LaneId": random.randint(1, 5),
            "PlateNumber": plate_number,
            "PlateState": plate_state_info["abbreviation"],  # Use abbreviation in JSON
            "TagAgencyId": f"{random.randint(1, 25):04d}" if random.random() > 0.5 else None,
            "TagNumber": f"{random.randint(1000000000, 9999999999)}" if random.random() > 0.5 else None,
            "VehicleClassId": random.randint(1, 5),
            "AxleCount": random.randint(1, 6),
            "CameraId": random.randint(1001, 1006),
            "CameraOrientation": random.choice(["F", "R"]),
            "DirectionId": random.randint(1, 2),
            "Latitude": round(random.uniform(40.0, 42.0), 6),  # Example latitude range
            "Longitude": round(random.uniform(-88.0, -86.0), 6),  # Example longitude range
            "vehicle_images": vehicle_images,
            "plate_types": plate_types,
        }
        batch.append(record)
        record_count += 1

        
        # Send or save in batches
        if len(batch) == batch_size:
            if USE_AZURE_SERVICE_BUS:
                send_to_azure_service_bus(batch)
            else:
                file_name = os.path.join(TRXDATA_DIR, f"batch_{uuid.uuid4().hex}.json")
                with open(file_name, "w") as file:
                    json.dump(batch, file, indent=4)
                print(f"Batch saved to file: {file_name}")
            batch = []

    # Process any remaining records
    if batch:
        if USE_AZURE_SERVICE_BUS:
            send_to_azure_service_bus(batch)
        else:
            file_name = os.path.join(TRXDATA_DIR, f"batch_{uuid.uuid4().hex}.json")
            with open(file_name, "w") as file:
                json.dump(batch, file, indent=4)
            print(f"Batch saved to file: {file_name}")

    print(f"Total records generated: {record_count}")

if __name__ == "__main__":
    start_date = datetime(2024, 12, 18)
    end_date = datetime.now()
    average_daily_volume = 10
    generate_traffic_data(start_date, end_date, average_daily_volume)
