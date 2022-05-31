import arrow

def get_week(dt):
    mydate = arrow.get(dt)
    start = mydate.floor('week')
    end = mydate.ceil('week')
    return arrow.Arrow.range('day', start, end)
