#!/usr/bin/python3

import pathlib as pl
import time, argparse, threading, queue

ROOTDIR='/sys/bus/w1/devices'

import logging
logging.basicConfig(filename='/home/pi/rrlog.log',level=logging.DEBUG)

class ds18b20():
    """
    simple class for ds18b20 temperature sensors.
    
    given the sensor's name (28-.....), the read function can be called to get the current temperature.
    
    It tracks the number of successful and failed reads
    """
    def __init__(self, name, offset=0.0):
        """
        sets up a sensor instance for a single sensor
        
        name    : the sensor's 1-wire name (e.g. 28-nnnnnnnn)
        
        offset  : an offset for the read value to allow very basic calibration
        """
        self.name=name
        self.rdr=pl.Path(ROOTDIR)/name/'w1_slave'
        self.goodreads=0
        self.badreads=0
        self.offset=offset
        self.lasterror=None

    def read(self):
        """
        attempts to read from the sensor.
        
        returns the temperature (float) in centigrade if the read was OK or None if the read failed
        """
        with self.rdr.open('r') as trdr:
            l=trdr.readline().strip()
            if l.endswith('YES'):
                ls=trdr.readline().split('t=')
                tval=int(ls[1])/1000
                self.goodreads+=1
                return tval
            else:
                self.badreads+=1
                self.lasterror=l
                return None

def find_devices(devtype='28'):
    """
    returns a list of the ds18b20 sensors found
    """
    devbase=pl.Path(ROOTDIR)
    if devbase.exists():
        devs=[]
        for onedev in devbase.iterdir():
            if onedev.name.startswith(devtype):
                devs.append(ds18b20(onedev.name))
        return devs
    else:
        raise RuntimeError('1-wire directory /sys/bus/w1/devices not found')

class rundev():
    def __init__(self, dev):
        self.dev=dev
        self.running=True

    def runloop(self, dataq, errq, tick=5, startround=5):
        nexttick=float(((time.time()+.5)//startround)*startround)+startround
        starttime=time.time()
        sleeptime=0
        try:
            while self.running:
                delay=nexttick-time.time()
                if delay > 0:
                    sleeptime += delay
                    time.sleep(delay)
                    v=self.dev.read()
                    if v is None:
                        errq.put((nexttick, self.dev.name, dev.lasterror))
                    else:
                        dataq.put((nexttick, self.dev.name, v))
                else:
                    errq.put((time.time(), devs[0].name, 'overrun %4.3f' % -delay))
                nexttick += tick
            errq.put((time.time(), self.dev.name, 'elapsed time: %6.2f, sleep time: %6.2f' % (time.time()-starttime, sleeptime)))
        except:
            logging.debug('ooops!', exc_info=True)

class csvfilewriter():
    def __init__(self, devorder, csvfile, squash):
        self.targname=csvfile.with_suffix('').name
        self.folder=csvfile.parent
        self.devorder=devorder
        self.squash=squash!=0
        self.sigchange=squash
        if self.squash:
            self.lastvals=[999]* len(self.devorder)
        cands=sorted(self.folder.glob(self.targname+ '*'))
        if len(cands)==0:
            donew=True
            logging.info('no existing file - new file')
        else:
            lastcand=cands[-1]
            donew=True
            with lastcand.open('r') as cr:
                ls=cr.readline().split(',')
            if len(ls) == len(self.devorder)+1:
                lss=[i.strip() for i in ls]
                if lss[0]=='time':
                    for ix, dn in enumerate(lss[1:]):
                        if self.devorder[ix]!=dn:
                            break
                            logging.info('unmatched names - new file')
                    else:
                        donew=False
        if donew:
            self.startfile()
        else:
            self.csvfile=lastcand
            logging.info('continue file %s' % lastcand)
        self.lastwrite=0

    def startfile(self):
        fn='%s_%s' % (self.targname, time.strftime('%y-%m-%d_%H:%M:%S',time.localtime()))
        self.csvfile=(self.folder/fn).with_suffix('.csv')
        self.csvfile.parent.mkdir(parents=True, exist_ok=True)
        with self.csvfile.open('w') as cw:
            cw.write('time, %s\n' % ', '.join([name for name in self.devorder]))
        self.lastwrite=0
        logging.info('new file created: %s' % str(self.csvfile))

    def writerec(self, tstamp, devdata):
        vals=[devdata[dname] for dname in self.devorder]
        if not self.csvfile is None:
            if not self.lastwrite==0:
                oldti=time.localtime(self.lastwrite)
                newti=time.localtime(tstamp)
                if newti.tm_hour<oldti.tm_hour:
                    self.startfile()
                    self.lastwrite=0
            if not self.squash or tstamp-self.lastwrite >= 60:
                sigchanges=True
            else:
                sigchanges=False
                for vi,v in enumerate(vals):
                    if not v is None and abs(v-self.lastvals[vi]) >= self.sigchange:
                        sigchanges=True
                        break
            if sigchanges:
                for ix, v in enumerate(vals):
                    if not v is None:
                        self.lastvals[ix]=v
                    vstr=', '.join([' ' if v is None else '%5.1f' % v for v in self.lastvals])
                with self.csvfile.open('a') as cw:
                    cw.write('%s, %s\n' % (time.strftime('%y-%m-%d %H:%M:%S', time.localtime(tstamp)), vstr))
                self.lastwrite=tstamp
        else:
            logging.debug('no csvfile')

class gather():
    """
    assembles readings into complete sets and writes a csv file with option to skip similar readings.
    The first line is a header with the name of each device 
    """
    def __init__(self, devorder, console, writers, forcewrite=60):
        """
        devorder    : list of device names in the order they are written to each line
        
        console     :  if True, each incoming record is written to stdout (note records may have blank entries
        
        writers     : list of writer objects to handle data
        
        
        forcewrite  : if squash is > 0 then even if no value has changes, a record will be written at least every 'forcewrite' seconds.
                      for example, this shows the sensors are still being read and makes it easier to plot graphs from the data
        
        """
        self.devorder=devorder
        self.console=console
        self.writers=writers
        
    def writerec(self, tstamp, devdata):
        vals=[devdata[dname] for dname in self.devorder]
        for writer in self.writers:
            writer.writerec(tstamp, devdata)        
        if self.console:
            vstr=', ' + ', '.join([' ' if v is None else '%5.1f' % v for v in vals])
            print(time.strftime('%H:%M:%S', time.localtime(tstamp)) +vstr)

def validtick(tickstr):
    try:
        tick=float(tickstr)
    except:
        raise argparse.ArgumentTypeError('tick parameter must be an integer')
    if tick < 1:
        raise argparse.ArgumentTypeError('tick parameter must 1 or more')

if __name__=='__main__':
    argp=argparse.ArgumentParser(description='simple logger for 1-wire sensors')
    argp.add_argument('--datafile', '-d', default='sensors', help='base for filenames to record data')
    argp.add_argument('--console', '-c', help='include  with any value for live output to console')
    argp.add_argument('--tick', '-t', default=5, type=validtick, help='tick interval - target time in seconds between readings. 2 seconds is the fastest reasonable time')
    args=argp.parse_args()
    devs=find_devices() 
    dqueue=queue.Queue()
    errqueue=queue.Queue()
    readers=[rundev(dev) for dev in devs]
    devnames=[dev.name for dev in devs]
    logging.info('start using ddevices %s' % devnames)
    threads=[threading.Thread(target=areader.runloop, kwargs={'dataq':dqueue, 'errq':errqueue, 'tick':args.tick, 'startround':5}) for areader in readers]
    for t in threads:
        t.start()
    csvwriter = csvfilewriter(devorder=devnames, csvfile=pl.Path(pl.Path(args.datafile).expanduser()), squash=.2)
    writer=gather(devorder=devnames, console=not args.console is None, writers=[csvwriter])
    devrec={dev.name:None for dev in devs}
    try:
        while True:
            time.sleep(1)
            logging.debug('try read')
            try:
                tstamp, devname, value = dqueue.get(block=True, timeout=.7)
                logging.debug('%s dev %s is %3.2f' % (time.strftime('%X', time.localtime()), devname, value))
            except queue.Empty:
                tstamp=None
                logging.debug('empty queue')
            if not tstamp is None:
                devrec[devname]=value
                time.sleep(.1)
                ntstamp=tstamp
                try:
                    while True:
                        ntstamp, devname, value = dqueue.get(block=False)
                        if abs(ntstamp-tstamp) < 1 and devrec[devname] is None:
                            devrec[devname]=value
                            logging.debug('%s is %4.2f' % (devname, value))
                        else:
                            logging.debug('timeout writing record')
                            writer.writerec(tstamp, devrec)
                            devrec={dev.name:None for dev in devs}
                            devrec[devname]=value
                except queue.Empty:
                    writer.writerec(tstamp, devrec)
                    logging.debug('empty writing record')
                    devrec={dev.name:None for dev in devs}
                    tstamp=ntstamp
            tstamp=0
            while not tstamp is None:
                try:
                    tstamp, devname, error=errqueue.get(block=False)
                except queue.Empty:
                    tstamp=None
                if not tstamp is None:
                    logging.info('AAAAAAAA %s: %s' % (devname, error))
            
    except KeyboardInterrupt:
        pass
    except:
        logging.exception('!!!!')
    finally:
        for r in readers:
            r.running=False
        for t in threads:
            t.join()
    tstamp=0
    while not tstamp is None:
        try:
            tstamp, devname, error=errqueue.get(block=False)
        except queue.Empty:
            tstamp=None
        if not tstamp is None:
            print('AAAAAAAA', devname, error)
    logging.info('closing')
