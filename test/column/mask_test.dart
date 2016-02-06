library frame.test.mask;

import 'dart:math' show min, max, Random;

import 'package:test/test.dart';
import 'package:option/option.dart';
import 'package:frame/frame.dart';

//  class Filter(f: Int => Boolean) extends (Int => Boolean) {
//    Filter(f: Int => Boolean)
//    def apply(n: Int): Boolean = f(n)
//  }

typedef bool Filter(int a);

final Random r = new Random();

const maxl = 222;
const minl = 9;

maskTest() {
  Mask genMask() {
    var rows0 = new List<int>.generate(r.nextInt(maxl) + minl, (_) => 0);
    var rows = rows0.map((r) => r & 0xFFFF);
    return new Mask.from(rows);
  }

//  implicit val arbMask: Arbitrary[Mask] = Arbitrary(genMask)

  Filter genFilter() {
    List<int> mods0 = new List<int>.generate(r.nextInt(maxl) + minl, (_) => 0);
    Set<int> mods = mods0.map((m) => m & 0xFF).map((m) => m + 1).toSet();
    return (int n) => mods.any((m) => n % m == 0);
  }

//  implicit val arbFilter: Arbitrary[Filter] = Arbitrary(genFilter)

  int bit;
  Mask mask, a, b;
  Filter filter;
  List<int> bits0;
  setUp(() {
    bit = r.nextInt(maxl);
    a = genMask();
    b = genMask();
    mask = genMask();
    filter = genFilter();
    bits0 = new List<int>.generate(maxl, (_) => 0);
  });

  group("+ should", () {
    test("add 0 to empty mask", () {
      expect(Mask.empty + 0, equals(new Mask.from([0])));
    });

    test("set bit of value added", () {
      expect((mask + bit)[bit], isTrue);
    });
  });

  group("- should", () {
    test("unset bit of value removed", () {
      expect((mask - bit)[bit], isFalse);
    });

    test("shrink underlying array when top words zero'd out", () {
      var a = new Mask.from([1, 100]);
      expect((a - 100).max(), equals(new Some(1)));
    });
  });

  group("filter should", () {
    test("remove bits that are filtered out", () {
      expect(mask.filter(filter).toSet(), equals(mask.toSet().where(filter)));
    });
  });

  group("min should", () {
    test("return minimum set bit", () {
      expect(new Some(mask.toSet().reduce((a, b) => min(a, b))),
          equals(mask.min()));
    });

    test("return None for empty Mask", () {
      expect(Mask.empty.min(), equals(new None()));
    });
  });

  group("max should", () {
    test("return maximum set bit", () {
      expect(new Some(mask.toSet().reduce((a, b) => max(a, b))),
          equals(mask.max()));
    });

    test("return None for empty Mask", () {
      expect(Mask.empty.max(), equals(new None()));
    });
  });

  group("isEmpty", () {
    test("return empty iff the mask is empty", () {
      expect(mask.toSet().isEmpty, equals(mask.isEmpty()));
    });
  });

  group("foreach should", () {
    test("iterate over bits in increasing order", () {
      var bldr = new List<int>();
      mask.foreach((i) => bldr.add(i));
      expect(bldr, equals(new List.from(bldr)..sort()));
    });

    test("iterate only over values in the mask", () {
      var bldr = <int>[];
      mask.foreach((i) => bldr.add(i));
      expect(bldr.every((i) => mask[i]), isTrue);
    });

    test("iterate over all values in mask", () {
      var bits1 = bits0.map((b) => b & 0xFFFF); // Sane sizes
      var mask = new Mask.from(bits1);
      var bits = bits1.toSet();
      mask.foreach((i) => bits.remove(i));
      expect(bits, isEmpty);
    });
  });

  group("toSet should", () {
    test("round-trip Mask->Set->Mask", () {
      expect(mask, equals(new Mask.from(mask.toSet().toList())));
    });

    test("round-trip Set->Mask->Set", () {
      var bits = bits0.map((b) => b & 0xFFFF).toSet();
      expect(new Mask.from(bits.toList()).toSet(), equals(bits));
    });
  });

  group("| should", () {
    test("only contain bits from either arguments", () {
      var mask = (a | b);
      mask.toSet().forEach((i) {
        expect(a[i] || b[i], isTrue);
      });
    });

    test("contain all bits from both arguments", () {
      var mask = a | b;
      var bits = a.toSet()..addAll(b.toSet());
      expect(bits.every((i) => mask[i]), isTrue);
    });
  });

  group("& should", () {
    test("only contain bits contained in both arguments", () {
      var mask = (a & b);
      expect(mask.toSet().every((i) => a[i] && b[i]), isTrue);
    });

    test("contain all bits that are in both arguments", () {
      var mask = a & b;
      var bits = a.toSet().intersection(b.toSet());
      expect(bits.every((i) => mask[i]), isTrue);
    });

    test("shrink array if top zero'd out", () {
      var a = new Mask.from([1, 100]);
      var b = new Mask.from([1, 101]);
      expect((a & b).max(), equals(new Some(1)));
    });
  });

  group("-- should", () {
    test("not contain bits in right-hand side", () {
      var mask = a.dec(b);
      expect(b.toSet().every((i) => !mask[i]), isTrue);
    });

    test("contain bits in the lhs but not the rhs", () {
      var mask = a.dec(b);
      var setBits = a.toSet().difference(b.toSet());
      expect(setBits.every((i) => mask[i]), isTrue);
    });
  });

  group("equals should", () {
    test("always be equal for equivalent masks", () {
      var b = new Mask.from(a.toSet());
      expect(a, equals(b));
    });

    test("should not throw IOOE when size are equal but lengths are not", () {
      var a = new Mask.from([1, 2, 3]);
      var b = new Mask.from([1000, 1001, 2000]);
      expect((a == b), isNot(throws));
      expect((b == a), isNot(throws));
    });
  });
}

main() {
  for (var i = 0; i < 1; i++) {
    maskTest();
  }
}
