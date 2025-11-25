data = [1, [2, 3], 4, [5, 6]]

flat = [x
        for sub in data
        for x in (sub if isinstance(sub, list) else [sub])]

print(flat)   # [1, 2, 3, 4, 5, 6]
