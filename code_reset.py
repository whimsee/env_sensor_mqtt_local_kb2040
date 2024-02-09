import time, microcontroller

microcontroller.on_next_reset(microcontroller.RunMode.BOOTLOADER)
time.sleep(5)
microcontroller.reset()

