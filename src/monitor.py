from tqdm import tqdm
from threading import Timer, Thread
from time import sleep

class Monitor ():
    """A Monitor class used to keep track of the current progress of some functions.
    """
    def __init__(self, unit = 'data', finite_bar_format = '{n_fmt}/{total_fmt} [{bar}] - {elapsed} - {rate_fmt}', infinite_bar_format='{n_fmt}/∞ [{bar}] - {elapsed}'):
        """Initialize Monitor object.

        Args:
            unit (str, optional): unit label. Defaults to 'data'.
            finite_bar_format (str, optional): format of finite tqdm progress bar. Defaults to '{n_fmt}/{total_fmt} [{bar}] - {elapsed} - {rate_fmt}'.
            infinite_bar_format (str, optional): format of infinite tqdm progress bar. Defaults to '{n_fmt}/∞ [{bar}] - {elapsed}'.
        """
        self._finite_bar_format = finite_bar_format
        self._infinite_bar_format = infinite_bar_format
        self._ncols = 100
        self._unit = unit
        self._leave = False
        self._mode=0

        self._tqdm=None
        self._is_tqdm_thread_activate = False

    def start(self, total = None, unit_scale=None, mode=0):
        """Generate a tqdm progress bar based a given mode.

        Args:
            total (int, optional): a total tick of the progress bar. Defaults to None.
            unit_scale (int, optional): a scaling factor of progress bar. Defaults to None.
            mode (int, optional): mode of progress bar. 1 = finite progress bar, 2 = infinite progress bar, and 3 = pandas progress bar. Defaults to 0.
        """
        self._mode=mode

        #Finite mode
        if self._mode == 0:
            self._tqdm = tqdm(total=total, unit_scale=unit_scale, ncols=self._ncols, unit=self._unit ,leave=self._leave, bar_format=self._finite_bar_format)
        
        #Infinite mode
        elif self._mode == 1:
            self._tqdm = tqdm(total=100, ncols=self._ncols ,leave=self._leave, bar_format=self._infinite_bar_format)
            self._is_tqdm_thread_activate=True
            thread = Thread(target=self._auto_update, daemon=True)
            thread.start()
            
        #Panda mode
        elif self._mode == 2:
            tqdm.pandas(total=total, unit_scale=unit_scale, ncols=self._ncols, unit=self._unit ,leave=self._leave, bar_format=self._finite_bar_format)
    
    def stop(self):
        """Stop and clean up a tqdm progress bar.
        """
        self._tqdm.close()
        if self._mode==1:
            self._is_tqdm_thread_activate=False

    def update(self, n = 1):
        """Update a tqdm progress bar.

        Args:
            n (int, optional): the number of tick that the progress bar should be progress by. It can't be more than total. Defaults to 1.
        """
        if self._tqdm.n == self._tqdm.total:
            self._tqdm.n = 0
            self._tqdm.last_print_n = 0
            self._tqdm.refresh()
        else:
            self._tqdm.update(n)

    def _auto_update(self):
        """A self call update of a tqdm a progress bar. This is only applicable to mode = 1.
        """
        while self._is_tqdm_thread_activate:
            self.update()
            sleep(0.1)