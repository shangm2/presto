namespace java com.example.thrift.generated
namespace cpp test

struct TestMessage {
  1: string text;
  2: i32 number;
  3: list<string> items;
}
