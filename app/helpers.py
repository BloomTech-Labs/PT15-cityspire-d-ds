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
    