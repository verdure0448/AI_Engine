list = []
for i in range(36):
    list.append(i)


def row_data_rolling(_list, _window_size):
    rolling_list = []
    for i in range(len(_list)-_window_size):
        rolling_list.append(sum(_list[i:_window_size+i])/_window_size)

    return rolling_list

#print(row_data_rolling(list, 30))

list = list[-1:]
print(list)