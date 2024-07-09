# Imports
import time
import pickle


# 1. Define AI model for WQ parameter prediction: e.g. ChlA. The model can be more complex (e.g. Class). The parameters need to be defined in this way
def AI_model_example(B01, B02, B03, B04, B05, B06, B07, B08, B8A, B09, B11, B12):
    """
    Example function for e.g. ChlA AI prediction model. The input parameters are bands from Sentinel-2 image (L2A level) or values for particular pixel.
    The values are INT in default (e.i. original values from the image). The numpy arrays can ce used.

    :param B01: Surface spectral reflectance (0 to 1) for particular pixel or for some area of the image (float)
    :param B02: ...
    :param B03: ...
    :param B04: ...
    :param B05: ...
    :param B06: ...
    :param B07: ...
    :param B08: ...
    :param B8A: ...
    :param B09: ...
    :param B11: ...
    :param B12: ...
    :return: Value or Numpy array
    """

    # imports --> define imports here
    from sklearn.linear_model import LinearRegression

    # Do some work, e.g.
    prediction = B05 / B04
    time.sleep(5)

    return prediction


# 2. Serialize the model to pickle file. The file will be used for predictions afterwards.
# We can store various models in the database (e.g. for particular WQ parameter and water reservoir id).
with open('AI_model_example.pkl', 'wb') as f:
    pickle.dump(AI_model_example, f)

# 3. Load the model from pickle file - will be done in the calculation script. In this place the pickle file is a blackbox.
with open('AI_model_example.pkl', 'rb') as f:
    AI_model_example = pickle.load(f)

chla_conc = AI_model_example(B01, B02, B03, B04, B05, B06, B07, B08, B8A, B09, B11, B12)