# gen_crime_score translates a scaled crime score to a user
#    friendly score value (5-best to 1-worst)
def gen_crime_score(val):
    # Highest scaled crime score -> worst crime rating
    if val >= 0.80:
        return 1
    
    if val >= 0.60:
        return 2

    if val >= 0.40:
        return 3

    if val >= 0.20:
        return 4

    # Lowest scaled crime score -> best crime rating
    return 5

# generates a rent_score based on quantiles of all rent rates
def get_rent_score(avg_rent):
    buckets = [ 742. , 1203.6, 1364. , 1535.6, 1721.2, 2993. ]
    if buckets[0] <= avg_rent <= buckets[1]:
        score = 5
    elif avg_rent <= buckets[2]:
        score = 4
    elif avg_rent <= buckets[3]:
        score = 3
    elif avg_rent <= buckets[4]:
        score = 2
    elif avg_rent <= buckets[5]:
        score = 1
    return score    