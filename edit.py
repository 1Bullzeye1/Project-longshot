class Abc:
    def _init_(self,x,y):
        self.x=x
        self.y=y
        print("x:",self.x)
        print("y:",self.y)
        print("today is sunday")
    def xyz(self):
        print("Hi I am Parent class")
    def mnr(self):
        print("Hi i am team BW")
class Pqr(Abc):

    def _init_(self,x,y,z): #constructor override
        self.x = x
        self.y=y
        self.z=z
        # print("z:",z)
        # super()._init_(x,y)
        print("python batch 2022")
    def abc(self):
        print("Hello {},{},{}".format(self.x,self.y,self.z),)
    def xyz(self): #method override
        super().xyz()
        print("hi i am child class")
a=Pqr()

a.abc()