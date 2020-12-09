import pandas as pd
import re
from datetime import timedelta

def splitDurations(duration: str, start: bool = True):
    """ Split times of the form `09.00-15.00` returning 09.00 
        if `start` else 15.00"""
    return duration.split('-')[0 if start else 1]

def formatCoworkerDescription(
    coworkers: pd.DataFrame, total_string_space:int = 100
    ) -> str:
    """ Pretty print the name and shift of coworkers in description """
    descriptions = coworkers.replace('', None).dropna().to_dict()
    return '\n'.join([
                name+ ' '*(total_string_space-len(name)-len(shift))+shift 
                for name, shift in descriptions.items()
            ])

def timedeltaString(timestring: str):
    """ convert times like 09.30 to timedelta(hours=9, minutes=30)"""
    if timestring:
        hours, minutes = map(int, re.split(r'\D+', timestring))
    else:
        hours, minutes = 0, 0 
    return timedelta(hours=hours, minutes=minutes)
