"""trade session and date manager decorator."""
import time
import functools

def ts_manager(fun):
    """decorator creator"""
    @functools.wraps(fun)
    def decorator(*args, **kwargs):
        """function accepts trade_session_id and target_date"""
        tdate = kwargs.get('tdate')
        print('%s%s' % (fun.__name__, (' for date %s' % tdate) if tdate else ''))
        start_time = time.time()

        try:
            res = fun(*args, **kwargs)
        except TypeError:
            res = fun(*args)

        print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))
        return res
    return decorator
