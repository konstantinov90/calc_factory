import mat4py



c = mat4py.loadmat('testmat4py.mat')
a = c['b']
print(c)
# mat4py.savemat('c.mat', c)
mat4py.savemat('a.mat', a)
