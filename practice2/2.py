from os.path import getsize
from numpy import savez, savez_compressed, load

points_filename = 'data/points_9_2.npz'
compressed_points_filename = 'data/compressed_points_9_2.npz'
limit = 500 + 9
x, y, z = [], [], []
matrix = load('data/matrix_9_2.npy')

for i in range(len(matrix)):
    for j in range(len(matrix)):
        if matrix[i][j] > limit:
            x.append(i)
            y.append(j)
            z.append(matrix[i][j])

savez(points_filename, x=x, y=y, z=z)
savez_compressed(compressed_points_filename, x=x, y=y, z=z)

print(f'Points file size: {getsize(points_filename)}\n'
      f'Compressed points file size: {getsize(compressed_points_filename)}')
