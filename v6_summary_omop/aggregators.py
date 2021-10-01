import math
import os

from v6_summary_omop.constants import *
from v6_summary_omop.utils import compare_with_minimum

def count(results):
    """ Calculates the global count.
    """
    count = sum([result[COUNT_FUNCTION] for result in results])
    return compare_with_minimum(count)

def maximum(results):
    """ Calculates the global maximum.
    """
    return max([result[MAX_FUNCTION] for result in results])

def minimum(results):
    """ Calculates the global minimum.
    """
    return min([result[MIN_FUNCTION] for result in results])

def average(results):
    """ Calculates de global average.
    """
    count = 0
    total = 0
    for result in results:
        count += result[COUNT_FUNCTION]
        total += result[SUM_FUNCTION]
    return total/count

def pooled_std(results):
    """ Calculated the pooled standard deviation. 
    """
    sample_size = 0
    weighted_std = 0
    for result in results:
        sample_size += result[COUNT_FUNCTION]
        weighted_std += (result[COUNT_FUNCTION] - 1) * (result[STD_SAMP_FUNCTION] ** 2)
    return math.sqrt(weighted_std / (sample_size - len(results)))

def histogram_aggregator(results):
    """ Aggregate the histograms. 
    """
    histogram = {}
    for result in results:
        for bin in result[HISTOGRAM]:
            if bin[0] not in histogram:
                histogram[bin[0]] = 0
            histogram[bin[0]] += int(bin[1])
    return {key: compare_with_minimum(value) for key, value in histogram.items()}

def boxplot(results):
    """ Aggregate the quartiles by node providing the information to
        build a boxplot.
    """
    aggregated_quartiles = {}
    for result in results:
        quartiles = result[QUARTILES]
        aggregated_quartiles[quartiles[0]] = {
            MIN_FUNCTION: result[MIN_FUNCTION],
            "q1": quartiles[1],
            "median": quartiles[2],
            "q3": quartiles[3],
            "lower_bound": quartiles[4],
            "upper_bound": quartiles[5],
            "lower_outliers": compare_with_minimum(quartiles[6]),
            "upper_outliers": compare_with_minimum(quartiles[7]),
            MAX_FUNCTION: result[MAX_FUNCTION]
        }
    return aggregated_quartiles

def sum_null(results):
    """ Calculate the total number of null values.
    """
    return sum([result[COUNT_NULL][0] for result in results])

def count_discrete(results):
    """ Count the occurences for each discrete value.
    """
    total_count = {}
    for result in results:
        for count in result[COUNT_DISCRETE]:
            if count[0] not in total_count:
                total_count[count[0]] = 0
            total_count[count[0]] += int(count[1])
    
    count_minimum = int(os.getenv(COUNT_MINIMUM) or COUNT_MINIMUM_DEFAULT)
    for key in total_count.keys():
        if total_count[key] < count_minimum:
            total_count[key] = f"< {count_minimum}"
    return total_count

def cohort_aggregator(results):
    """ Aggregate the number of individuals included in the cohort
        definition.
    """
    count = {}
    for result in results:
        count[result[0]] = result[1]
    return count
