from functools import wraps
import time
def try_decorator(function):
    @wraps(function)
    def wrapper_function(*args, **kwargs):
        while True:
            try:
                result = function(*args, **kwargs)

                if result:
                    return result
                else:
                    break

            except Exception as e:
                print('Error', e, 'Repeating!...')
                time.sleep(5)

    return wrapper_function
