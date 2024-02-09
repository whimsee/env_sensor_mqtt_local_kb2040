import time, busio, board, alarm, microcontroller, asyncio, pwmio
from digitalio import DigitalInOut
import adafruit_ht16k33.segments

import adafruit_bus_device
from adafruit_pm25.i2c import PM25_I2C
import adafruit_scd4x
import adafruit_sgp40

from adafruit_esp32spi import adafruit_esp32spi
import adafruit_esp32spi.adafruit_esp32spi_socket as socket
import adafruit_requests as requests

import adafruit_minimqtt.adafruit_minimqtt as MQTT


def connected(client, userdata, flags, rc):
    # This function will be called when the client is connected
    # successfully to the broker.
    # print(f"Connected to MQTT broker! Listening for topic changes on {default_topic}")
    # Subscribe to all changes on the default_topic feed.
    client.subscribe("home_pressure")


def disconnected(client, userdata, rc):
    # This method is called when the client is disconnected
    print("Disconnected from MQTT Broker!")


def message(client, topic, message):
    """Method callled when a client's subscribed feed has a new
    value.
    :param str topic: The topic of the feed with a new value.
    :param str message: The new value
    """
    print(f"New message on topic {topic}: {message}")
    try:
        if topic == "home_pressure":
            pressure = int(message)
            scd4x.set_ambient_pressure = pressure
            print(topic, pressure)
    except Exception as e:
        print(e)
        # Default pressure = 1013.25
        scd4x.set_ambient_pressure = 1013.25

def mqtt_publish(topic, message):
    try:
        mqtt_client.publish(feed_main + topic, message)
    except ConnectionError:
        print(topic, "Connection error")

def read_pm25(pm25):
    read_tries = 0
    read_attempt_limit = 5

    while read_tries < read_attempt_limit:
        try:
            particles = pm25.read()
            break
        except RuntimeError:
            print("RuntimeError while reading pm25, trying again. Attempt: ", read_tries)
            read_tries += 1
            time.sleep(0.1)
    if read_tries >= read_attempt_limit:
        # We tried too many times and it didn't work. Skip.
        return False
    return particles

def get_scd_reading():
    while True:
        if scd4x.data_ready:
            co2_reading = scd4x.CO2
            temperature_C = scd4x.temperature - temperature_offset
            base_hum = scd4x.relative_humidity
#             print(temperature_C)
            break
    return co2_reading, temperature_C, base_hum

# Connect to WIFI and setup MQTT client
try:
    from secrets import secrets
except ImportError:
    print("WiFi secrets are kept in secrets.py, please add them there!")
    raise

esp32_cs = DigitalInOut(board.D5)
esp32_ready = DigitalInOut(board.D3)
esp32_reset = DigitalInOut(board.D4)

spi = busio.SPI(board.SCK, board.MOSI, board.MISO)
esp = adafruit_esp32spi.ESP_SPIcontrol(spi, esp32_cs, esp32_ready, esp32_reset)

# Setup a feed named `testfeed` for publishing.
feed_main = "kb2040_env/feeds/"

requests.set_socket(socket, esp)

if esp.status == adafruit_esp32spi.WL_IDLE_STATUS:
    print("ESP32 found and in idle mode")

print("Connecting to AP...")
while not esp.is_connected:
    try:
        esp.connect_AP(secrets["ssid"], secrets["password"])
    except Exception as e:
        print("could not connect to AP, resetting ", e)
        time.sleep(20)
        microcontroller.reset()

    print("Connecting to MQTT Broker")
    try:
        # Initialize MQTT interface with the esp interface
        MQTT.set_socket(socket, esp)

        # Set up a MiniMQTT Client
        mqtt_client = MQTT.MQTT(
            broker=secrets["mqtt_broker"], username=secrets["mqtt_username"], password=secrets["mqtt_password"]
        )

        # Setup the callback methods above
        mqtt_client.on_connect = connected
        mqtt_client.on_disconnect = disconnected
        mqtt_client.on_message = message
        # Connect the client to the MQTT broker.
        mqtt_client.connect()
        print("MQTT connected!")
    except Exception as e:
        print(e)
        raise

### Sensors ###

# ==============

print("Init particulate")
i2c = busio.I2C(board.SCL, board.SDA, frequency=100000)
reset_pin = None

display = adafruit_ht16k33.segments.Seg7x4(i2c)

pm25 = PM25_I2C(i2c, reset_pin)

print("Init CO2, temp, hum, get pressure compensation")
sgp = adafruit_sgp40.SGP40(i2c)
scd4x = adafruit_scd4x.SCD4X(i2c)

print("Set constants")
# You will usually have to add an offset to account for the temperature of
# the sensor. This is usually around 5 degrees but varies by use. Use a
# separate temperature sensor to calibrate this one.
temperature_offset = 5

## Check elevation in location
scd4x.altitude = 263

# Time-related (data per 30 mins in ticks, pressure/hour in seconds)
# UPLOAD_TIME = 1766.5
UPLOAD_TIME = 300
PRESSURE_TIME = 3600

# Set inital pressure
mqtt_client.loop()
scd4x.start_periodic_measurement()

# LEDs
ON = 300
OFF = 0
index_led = pwmio.PWMOut(board.A0)
# index_led.duty_cycle = ON

part_led = pwmio.PWMOut(board.A1)
# part_led.duty_cycle = ON

CO2_led = pwmio.PWMOut(board.A2)
# CO2_led.duty_cycle = ON

time.sleep(1)

# Async-specific classes and functions
class data_box:

    def __init__(self, co2, temperature, temperature_C, humidity, gas_index, part_03, part_10, part_25, part_env_25):
        self.co2 = co2
        self.temperature = temperature
        self.temperature_C = temperature_C
        self.humidity = humidity
        self.part_03 = part_03
        self.part_10 = part_10
        self.part_25 = part_25
        self.part_env_25 = part_env_25
        self.gas_index = gas_index
#         self.ticks = ticks

async def upload_data(data):
    while True:
        if not esp.is_connected:
            print("No internet. Resetting")
            microcontroller.reset()
        # data.ticks = 0
        mqtt_client.connect()
        to_upload = (data.co2, data.part_03, data.part_10, data.part_25, data.part_env_25, data.temperature, round(data.humidity), data.gas_index)
        print(to_upload)
        print("Uploading values to feed")
        start_time = time.monotonic()

        mqtt_publish("co2",to_upload[0])
        mqtt_publish("particles-03um",to_upload[1])
        mqtt_publish("particles-10um",to_upload[2])
        mqtt_publish("particles-25um",to_upload[3])
        mqtt_publish("env-concentration-25um",to_upload[4])
        mqtt_publish("temperature",to_upload[5])
        mqtt_publish("humidity",to_upload[6])
        mqtt_publish("gas-index",to_upload[7])

        print("Upload finished")
        time_interval = time.monotonic() - start_time
        mqtt_client.disconnect()
        await asyncio.sleep(UPLOAD_TIME - time_interval)

async def sgp_gas_reading(data):
    await asyncio.sleep(0)
    while True:
        data.gas_index = sgp.measure_index(data.temperature_C, data.humidity)
#         print(data.gas_index)
#         print("gas get")
        if data.gas_index > 120:
            index_led.duty_cycle = ON
        else:
            index_led.duty_cycle = OFF
#         if data.ticks >= UPLOAD_TIME:
#             await upload_data(data)
#         data.ticks += 1
#         print(data.ticks)
        await asyncio.sleep(1)

async def take_pressure():
    while True:
        await asyncio.sleep(PRESSURE_TIME)
        mqtt_client.connect()
        mqtt_client.loop()
        mqtt_client.disconnect()

async def main_readings(data):
    await asyncio.sleep(0)
    while True:
        co2_reading, temperature_C, base_hum = get_scd_reading()
        data.co2 = co2_reading
        data.temperature_C = temperature_C
        data.temperature = round(temperature_C * 9 / 5 + 32)
        data.humidity = base_hum
        if data.co2 > 1000:
            CO2_led.duty_cycle = ON
        else:
            CO2_led.duty_cycle = OFF
#         print("C02 get")
        await asyncio.sleep(0)

        particles = read_pm25(pm25)
        if particles:
            data.part_03 = particles['particles 03um']
            data.part_10 = particles['particles 10um']
            data.part_25 = particles['particles 25um']
            data.part_env_25 = particles['pm25 env']
        else:
            data.part_03 = 0
            data.part_10 = 0
            data.part_25 = 0
            data.part_env_25 = 0

        if data.part_env_25 > 35:
            part_led.duty_cycle = ON
        else:
            part_led.duty_cycle = OFF
#         print("Particles get")
        display.fill(0)
        display.print(data.temperature)
        await asyncio.sleep(1)

async def main():
    print("Take initial readings. Warm up sensor.")

    i = 0
    while i < 150:
        y = i % 5
        if y == 0:
            co2_reading, temperature_C, base_hum = get_scd_reading()
        sgp_gas_index = sgp.measure_index(temperature_C, base_hum)
        print(i,sgp_gas_index)
        i += 1
        time.sleep(1)

    # =================
    print("Compile readings")

    particles = read_pm25(pm25)
    if particles:
        particles_03um_reading = particles['particles 03um']
        particles_10um_reading = particles['particles 10um']
        particles_25um_reading = particles['particles 25um']
        env_25um_reading = particles['pm25 env']
    else:
        particles_03um_reading = 0
        particles_10um_reading = 0
        particles_25um_reading = 0
        env_25um_reading = 0

    co2_reading, temperature_C, base_hum = get_scd_reading()
    temp_reading = round(temperature_C * 9 / 5 + 32)
    sgp_gas_index = sgp.measure_index(temperature_C, base_hum)

    data_holder = data_box(co2_reading, temp_reading, temperature_C, base_hum, sgp_gas_index, particles_03um_reading, particles_10um_reading, particles_25um_reading, env_25um_reading)

    main_readings_task = asyncio.create_task(main_readings(data_holder))
    set_pressure_task = asyncio.create_task(take_pressure())
    gas_read_task = asyncio.create_task(sgp_gas_reading(data_holder))
    upload_task = asyncio.create_task(upload_data(data_holder))

    # This will run forever, because neither task ever exits.
    print("Start tasks")
    await asyncio.gather(upload_task, main_readings_task, gas_read_task, set_pressure_task)

asyncio.run(main())
