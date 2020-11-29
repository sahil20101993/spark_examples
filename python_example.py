class car():

    # init method or constructor
    def __init__(self, model, color):
        self.model = model
        self.color = color

    def show(self):
        """
        Hi  my name is sahil

        """
        print("Model is", self.model)
        print("color is", self.color)

    # both objects have different self which



def main():

    audi = car("audi a4", "blue")
    ferrari = car("ferrari 488", "green")
    audi.show()  # same output as car.show(audi)
    # ferrari.show()  # same output as car.show(ferrari)

# Behind the scene, in every instance method
# call, python sends the instances also with
# that method call like car.show(audi)

if __name__ == "__main__":
    main()

else:
    print ("Executed when imported")
