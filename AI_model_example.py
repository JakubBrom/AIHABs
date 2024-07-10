# Imports
import dill             # please use dill instead of pickle
import joblib
import numpy as np


# 1. Define AI model for WQ parameter prediction from S2 L2A data: e.g. ChlA. The model can be more complex (e.g. Class).
# The parameters need to be defined in this way
class AI_model_example():
    def __init__(self, model_path):
        self.model = joblib.load(model_path)

    def preprocess(self, input_data):
        import numpy as np      # if some library is used within the function, it need to be defined here! I dont
        # know why ...

        # Do some work with data here
        processed_data = input_data
        processed_data = np.reshape(processed_data, (len(processed_data), 1))       # Reshape data for input to the
        # model if needed

        return processed_data

    def predict(self, input_data):
        # Predict the data using the model
        processed_data = self.preprocess(input_data)
        predicted_data = self.model.predict(processed_data)

        # Do some postprocessing here
        post_data = predicted_data

        return post_data

# 2. Serialize the model to pickle file. The file will be used for predictions afterwards.
# We can store various models in the database (e.g. for particular WQ parameter and water reservoir id).
model_path = 'model.joblib'
model_instance = AI_model_example(model_path)
with open('AI_model_example.pkl', 'wb') as f:
    dill.dump(model_instance, f)                    # use dill instead of pickle


#-----------------------------------------------------------------------------------------------------------------------
# 3. Load the model from pickle file - will be done in the calculation script. In this place the pickle file is a blackbox.
with open('AI_model_example.pkl', 'rb') as f:
    AI_model_example = dill.load(f)


# 4. Input data definition !!!
input_data = [B01, B02, B03, B04, B05, B06, B07, B08, B8A, B09, B11, B12]
# Surface spectral reflectances (0 to 1) for particular pixel or for some area of the image for particular bands (float)
# In AIHABs the bands are flattened Numpy arrays (1D) --> data selected for random points from the image within the
# reservoir, e.g. B01 = [0.15 0.122 0.243 0.321 ...]

# 5. Run the model
chla_conc = AI_model_example.predict(input_data)