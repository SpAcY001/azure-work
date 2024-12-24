def runner(brand, model="",year=2021,convertible=False):
    return (brand,str(year),str(convertible))

print(runner(model = "furiours", brand="Ampere")[1][1])