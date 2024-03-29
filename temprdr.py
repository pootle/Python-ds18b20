#!/usr/bin/python3

import pathlib as pl
import time, argparse, threading, queue
import json, sys

ROOTDIR='/sys/bus/w1/devices'

import logging
logpath = pl.Path.home()/'rrlog.log'
logging.basicConfig(filename=logpath,level=logging.INFO)

class ds18b20():
    """
    simple class for ds18b20 temperature sensors.
    
    given the sensor's name (28-.....), the read function can be called to get the current temperature.
    
    It tracks the number of successful and failed reads
    """
    def __init__(self, name, offset=0.0):
        """
        sets up a sensor instance for a single sensor
        
        name        : the sensor's 1-wire name (e.g. 28-nnnnnnnn)
        
        mappedname  : name to use when logging the data
        
        offset      : an offset for the read value to allow very basic calibration
        """
        self.name=name
        self.mappedname=name
        self.rdr=pl.Path(ROOTDIR)/name/'w1_slave'
        self.goodreads=0
        self.badreads=0
        self.offset=offset
        self.lasterror=None

    def setmapped(self, mappedname):
        self.mappedname=mappedname

    def read(self):
        """
        attempts to read from the sensor.
        
        returns the temperature (float) in centigrade if the read was OK or None if the read failed
        """
        with self.rdr.open('r') as trdr:
            l=trdr.readline().strip()
            if l.endswith('YES'):
                ls=trdr.readline().split('t=')
                tval=int(ls[1])/1000+self.offset
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
            logging.info('start device %s with tick %4.1f' %(self.dev.mappedname, tick))    
            while self.running:
                delay=nexttick-time.time()
                if delay > 0:
                    logging.debug('delay is %3.1f' % delay)
                    sleeptime += delay
                    time.sleep(delay)
                    v=self.dev.read()
                    if v is None:
                        errq.put((nexttick, self.dev.mappedname, dev.lasterror))
                    else:
                        dataq.put((nexttick, self.dev.mappedname, v))
                    nexttick += tick
                else:
                    tm=time.time()
                    errq.put((tm, self.dev.mappedname, 'overrun %4.3f' % -delay))
                    while tm > nexttick:
                        nexttick+=tick
                
            errq.put((time.time(), self.dev.mappedname, 'elapsed time: %6.2f, sleep time: %6.2f' % (time.time()-starttime, sleeptime)))
        except:
            logging.info('ooops!', exc_info=True)

class csvfilewriter():
    def __init__(self, devorder, datafile, squash, tempform='%5.2f', forcewrite=60):
        """
        sets up info for writing consistent csv file

        devorder    : list of device names in the order they are to be written on each line
        
        datafile    : root of the filename to write - local date / time will be appended to this
        
        squash      : if 0 every record is written, otherwise this is the difference in temp (on any 1 sensor) that will
                      cause a record to be written
        
        tempform    : format used to write each temperature - primarlily to restrict number of decimal places written, also 
                      this makes it easier to visually scan lines from the file as all readings will line up
        
        forcewrite  : if squash is > 0 then a record will always be written after this much time
        """
        datafilepath=pl.Path(datafile).expanduser()
        self.targname=datafilepath.with_suffix('').name
        self.folder=datafilepath.parent
        self.devorder=devorder
        self.tempform=tempform
        self.squash=squash!=0
        self.sigchange=squash
        if self.squash:
            self.lastvals=[999]* len(self.devorder)
        self.forcewrite=forcewrite
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
            cw.write('time,%s\n' % ','.join([name for name in self.devorder]))
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
            if not self.squash or tstamp-self.lastwrite >= self.forcewrite:
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
                    vstr=','.join([' ' if v is None else self.tempform % v for v in self.lastvals])
                with self.csvfile.open('a') as cw:
                    cw.write('%s,%s\n' % (time.strftime('%y-%m-%d %H:%M:%S', time.localtime(tstamp)), vstr))
                self.lastwrite=tstamp
        else:
            logging.debug('no csvfile')

class gather():
    """
    assembles readings into complete sets and writes a csv file with option to skip similar readings.
    The first line is a header with the name of each device 
    """
    def __init__(self, devorder, console, writers):
        """
        devorder    : list of device names in the order they are written to each line
        
        console     :  if True, each incoming record is written to stdout (note records may have blank entries
        
        writers     : list of writer objects to handle data
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
    return tick

if __name__=='__main__':
    argp=argparse.ArgumentParser(description='simple logger for 1-wire sensors')
    argp.add_argument('--datafile', '-d', default='~/data/sensors', help='base for filenames to record data')
    argp.add_argument('--livelog', '-l', help='include for live output to console')
    argp.add_argument('--tick', '-t', default=5, type=validtick, help='tick interval - target time in seconds between readings. 2 seconds is the fastest reasonable time')
    argp.add_argument('--config', '-c', help='(optional) path to config file')
    args=argp.parse_args()
    if not args.config is None:
        confpath=pl.Path(args.config)
        if not confpath.exists():
            print('unable to find config file %s' % str(confpath), file=sys.stderr)
            sys.exit(1)
        else:
            try:
                with confpath.open('r') as cf:
                    config=json.load(cf)
            except:
                print('failed to load config from %s' % str(confpath))
                raise
    else:
        config={
            "tick": 20,
            "csvparams": {
                "squash":0.1,
                "tempform":" %5.2f",
                "forcewrite": 120,
                "datafile": "~/data/temperature-log"}
        }
    if not 'tick' in config:
        config['tick']=args.tick
    else:
        config['tick']=float(config['tick'])
    if not 'datafile' in config:
        config['datafile']=args.datafile
    devs=find_devices() 
    dqueue=queue.Queue()
    errqueue=queue.Queue()
    readers=[rundev(dev) for dev in devs]
    if 'namemap' in config:
        devids=[e[0] for e in config['namemap']]
        devnames=[e[1] for e in config['namemap']]
        nonames=[]
        for dev in devs:
            if dev.name in devids:
                dev.setmapped(devnames[devids.index(dev.name)])
            else:
                nonames.append(dev.name)
        devnames += nonames
    else:
        devnames=[dev.name for dev in devs]
    logging.info('start using devices %s' % (', '.join([dev.name if dev.name==dev.mappedname else '%s->%s' % (dev.name, dev.mappedname) for dev in devs])))
    threads=[threading.Thread(target=areader.runloop, kwargs={'dataq':dqueue, 'errq':errqueue, 'tick':config['tick'], 'startround':5}) for areader in readers]
    for t in threads:
        t.start()
    csvwriter = csvfilewriter(
                devorder=devnames,
                **config.get('csvparams',{}))
    writer=gather(devorder=devnames, console=not args.livelog is None, writers=[csvwriter])
    devrec={dn:None for dn in devnames}
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
                            devrec={dn:None for dn in devnames}
                            devrec[devname]=value
                except queue.Empty:
                    writer.writerec(tstamp, devrec)
                    logging.debug('empty writing record')
                    devrec={dn:None for dn in devnames}
                    tstamp=ntstamp
            tstamp=0
            while not tstamp is None:
                try:
                    tstamp, devname, error=errqueue.get(block=False)
                except queue.Empty:
                    tstamp=None
                if not tstamp is None:
                    logging.info('Driver report %s: %s' % (devname, error))
            
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
            print('Final driver report', devname, error)
    logging.info('closing')
