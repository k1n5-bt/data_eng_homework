import numpy as np
import json

matrix = np.load('data/matrix_9.npy')

json_result = {
    'sum': int(matrix.sum()),
    'avr': int(matrix.sum()) / len(matrix) ** 2,
    'sumMD': int(np.trace(matrix)),
    'avrMD': int(np.trace(matrix)) / len(matrix),
    'sumSD': int(np.trace(matrix[::-1])),
    'avrSD': int(np.trace(matrix[::-1])) / len(matrix),
    'max': int(matrix.max()),
    'min': int(matrix.min()),
}

np.save('data/norm_matrix_9.npy', matrix / matrix.sum())

with open('data/json_out_9', mode='w') as f:
    f.write(json.dumps(json_result))
