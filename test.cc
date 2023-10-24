#include <iostream>
#include <thread>
#include <mutex>

class MyClass {
public:
    MyClass(int* sharedData, std::mutex& sharedLock) : sharedData_(sharedData), sharedLock_(sharedLock) {}

    void modifySharedData() {
        std::lock_guard<std::mutex> lock(sharedLock_); // 使用传递的锁对象
        for(int i=0;i<500;i++)
              (*sharedData_)++; // 修改共享数据

    }

private:
    int* sharedData_;
    std::mutex& sharedLock_; // 传递的锁对象的引用
};

 template <class CLASS>
class My {
    public:
      My(CLASS classs) : myclass(std::move(classs)){}
      void acc() { myclass.modifySharedData(); }
    private:
      CLASS myclass;
};

int main() {
    int sharedValue = 0; // 共享数据
    std::mutex sharedMutex; // 创建一个锁对象

    MyClass classA(&sharedValue, sharedMutex); // 创建类实例A，传递共享数据的指针和锁对象
    MyClass classB(&sharedValue, sharedMutex); // 创建类实例B，传递共享数据的指针和锁对象
    My<MyClass> myA(std::move(classA));
    My<MyClass> myB(std::move(classB));

    std::thread threadA([&myA]() {
        myA.acc();
    });

    std::thread threadB([&myB]() {
        myB.acc();
    });

    threadA.join();
    threadB.join();

    std::cout << "Final sharedValue: " << sharedValue << std::endl;

    return 0;
}
