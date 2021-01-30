## Binary Search Algorithm:
##--------------- returns index location of input
def binary_search(input_array, value):
    low = 0
    high = len(input_array) -1
    while (low<=high):
        mid = (low+high)/2
        if input_array[mid] == value:
            return mid
        elif input_array[mid] < value:
            low = mid + 1
        else:
            high = mid - 1
    return -1

test_list = [1,3,9,11,15,19,29]
test_val1 = 25
test_val2 = 15
print binary_search(test_list, test_val1)
print binary_search(test_list, test_val2)




## FIBONACCI SEQUENCE FOR RECURSIVE FUNCTION
def get_fib(position):
    if position == 0 or position == 1:
        return position
    return get_fib(position - 1) + get_fib(position - 2)




locations = {'North America': {'USA': ['Mountain View']}}
locations = {'North America': {'USA': ['Mountain View']}}
locations['North America']['USA'].append('Atlanta')
locations['Asia'] = {'India': ['Bangalore']}
locations['Asia']['China'] = ['Shanghai']
locations['Africa'] = {'Egypt': ['Cairo']}

print(1)
usa_sorted = sorted(locations['North America']['USA'])
for city in usa_sorted:
    print(city)

print(2)
asia_cities = []
for countries, cities in locations['Asia'].iteritems():
    city_country = cities[0] + " - " + countries
    asia_cities.append(city_country)
asia_sorted = sorted(asia_cities)
for city in asia_sorted:
    print (city)

