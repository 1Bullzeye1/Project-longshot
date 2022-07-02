x = ("apple","banana","orange")
a = set(x)
y = ("orange","guava")
b = set(y)
print(a)
print(b)

print(a.intersection_update(b))
print(a.intersection(b))
print(a.symmetric_difference_update(b))
print(a.symmetric_difference(b))

