# Python-ds18b20
Python module to grab temperature readings from multiple ds18b20 1-wire sensors and log the data to a csv file

This app finds all the ds18b20 sensors on the 1-wire interface and logs them to a csv file at a specified period.

It uses multiple threads to allow mutiple sensors to be read relatively quickly (every few seconds).
(Reading each sensor takes aound 1.5 seconds, so serially reading several limits the speed at whch they can be read).

A new log file is automatically started at midnight each night.

Written to run originally on a Raspberry pi, it uses the linux 1-wire driver so should be pretty portable.

Run the app automatically on boot with (for example) crontab with a line similar to:

@reboot python3 /home/pi/gitbits/Python-ds18b20/temprdr.py -d ~/data/mmm 1>/home/pi/logs.log 2>/home/pi/logr.log
