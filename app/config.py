import os
from dotenv import load_dotenv

class WgtsCityScore:
    """
    WgtsCityScore models an object that stores
    default weights used to calculate an overall
    city quality of life score
    """

    def __init__(self):
        # Load environment variables
        load_dotenv()
        # Fetch environment variable values
        self.WEIGHT_CRIME   = float(os.getenv("WEIGHT_CRIME", 0.0))
        self.WEIGHT_WALK    = float(os.getenv("WEIGHT_WALK", 0.0))
        self.WEIGHT_AIR     = float(os.getenv("WEIGHT_AIR", 0.0))
        self.WEIGHT_RENT    = float(os.getenv("WEIGHT_RENT", 0.0))

        # Validate the weight values
        wgt_sum = self.WEIGHT_CRIME + \
            self.WEIGHT_WALK        + \
            self.WEIGHT_AIR         + \
            self.WEIGHT_RENT
    
        if wgt_sum != 1.0:
            # error: sum of weights not equal to 1.0
            print(f"ERROR: city score weights ({wgt_sum}) do not sum to 1.0. Using equal weights instead.")
            
            self.WEIGHT_CRIME   = 0.25
            self.WEIGHT_WALK    = 0.25
            self.WEIGHT_AIR     = 0.25
            self.WEIGHT_RENT    = 0.25

        # Construct a city weights dict
        self.weights_city = {
            "crime": self.WEIGHT_CRIME,
            "walk": self.WEIGHT_WALK,
            "air": self.WEIGHT_AIR,
            "rent": self.WEIGHT_RENT}

    def get_weights(self):
        return self.weights_city


# Construct a city score weights object
city_score_weights = WgtsCityScore()
