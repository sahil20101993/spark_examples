def smart_divide(func):
    """
    my name is sahil
    :param func:
    :return:
    """
    def inner(a, b):
        print("I am going to divide", a, "and", b)
        if b == 0:
            print("Whoops! cannot divide")
            return

        return func(a, b)
    return inner


@smart_divide
def divide(a, b):
    print(a/b)

print(smart_divide.__doc__)
divide(5,0)
divide=smart_divide(divide(5,0))
