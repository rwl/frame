/*
 * Copyright 2014 Pellucid Analytics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

part of frame;

/**
 * A `Mask` provides a dense bitset implementation. This replaces uses of
 * `BitSet`. The major difference is that we don't box the `Int`s.
 *
 * An explanation of some of the arithmetic you'll see here:
 *
 * We store the bits in array of words. Each word contains 64 bits and the
 * words are in order. So, the word containing bit `n` is `n &gt;&gt;&gt; 6` -
 * we simply drop the lower 6 bits (divide by 64). If `n` is set, then the bit
 * `n &amp; 0x3FL` (the last 6 bits - ie n % 64 if n was an unsigned int), in
 * word `bits(n &gt;&gt;&gt; 6)` will be `true`. We can check this by masking
 * the word with `1L &lt;&lt; (n &amp; 0x3FL)` and checking if the result is
 * non-zero.
 *
 * Note that we use shift-without-carry (`&gt;&gt;&gt;`) and intersection
 * (`&amp;`) to divide and mod by 64 instead of using `/` and `%` because they
 * do not behave correctly with negative numbers; they carry the sign through
 * in the result, and we want the absolute value.
 *
 * An invariant of the underlying `bits` array is that the highest order word
 * (ie. `bits(bits.length - 1)`) is always non-zero (except if `bits` has 0
 * length). This sometimes means we must *trim* the array for some operations
 * that could possibly zero out the highest order word (eg. intersection and
 * subtraction.
 */
class Mask /*extends (Int => Boolean)*/ {
//  import Mask.trim
  final Uint32List bits;
  final int size;

  Mask(this.bits, this.size);

  Option<int> max() {
    if (bits.length == 0) {
      return new None();
    } else {
      // An invariant of all Masks is that the highest order word always has
      // at least 1 bit set. So, we can just loop through all 64 bits in the
      // highest word (highest to lowest) and return the first set one.
      var hi = bits.length - 1;
      var word = bits[hi];
      var i = 64;
      while (i > 0) {
        i -= 1;
        if ((word & (1 << i)) != 0) {
          return new Some((hi << 6) | i);
        }
      }
      return new None();
    }
  }

  Option<int> min() {
    if (bits.length == 0) {
      return new None();
    } else {
      // We can cheat here and simply use `foreach` + `return`.
//      foreach((i) => return Some(i));
      return new None();
    }
  }

  bool isEmpty() => size == 0;

  void foreach(f(int a)) {
    var i = 0;
    while (i < bits.length) {
      var word = bits[i];
      var hi = i << 6;
      var lo = 0;
      while (lo < 64) {
        if ((word & (1 << lo)) != 0) {
          f(hi | lo);
        }
        lo += 1;
      }
      i += 1;
    }
  }

  static List<int> _copyOf(List<int> l, int size) {
    var r = new Uint32List(size);
    for (var i = 0; i < math.min(l.length, size); i++) {
      r[i] = l[i];
    }
  }

  Mask operator |(Mask that) {
    // We can simply | all the words together. The new bits array will be as
    // long as the largest of this.bits and that.bits.
    var size = math.max(bits.length, that.bits.length);
    // copyOf will zero out the top bits, which is exactly what we want.
    var bits0 = _copyOf(that.bits, size);
    var i = 0;
    while (i < bits.length) {
      bits0[i] |= bits[i];
      i += 1;
    }
    Mask.fromBits(bits0);
  }

  Mask operator &(Mask that) {
    // We can simply & all the words together. The new bits array will be as
    // long as the shortest of this.bits and that.bits.
    var size = math.min(bits.length, that.bits.length);
    var bits0 = _copyOf(that.bits, size);
    var i = 0;
    while (i < bits0.length) {
      var word = bits0[i] & bits[i];
      bits0[i] = word;
      i += 1;
    }
    return Mask.fromBits(bits0);
  }

  Mask inc(Mask that) => this | that;

  Mask dec(Mask that) {
    var bldr = new MaskBuilder();
    foreach((i) {
      if (!that(i)) {
        bldr.add(i);
      }
    });
    return bldr.result();
  }

  Mask operator +(int n) {
//    var hi = n >>> 6           // The offset of the word this bit is in.
    var hi = (n & 0xFFFFFFFF) >> 6;
    var bit = 1 << (n & 0x3F); // The bit position in the word n is in.

    if (hi < bits.length && (bits[hi] & bit) != 0) {
      // The bit is already set, so we're done!
      return this;
    } else {
      var len = math.max(bits.length, hi + 1);
      var bits0 = _copyOf(bits, len);
      bits0[hi] |= bit;
      return new Mask(bits0, size + 1);
    }
  }

  Mask operator -(int n) {
//    var hi = n >>> 6;
    var hi = (n & 0xFFFFFFFF) >> 6;
    var bit = 1 << (n & 0x3F);

    if (hi < bits.length && (bits[hi] & bit) != 0) {
      var bits0 = _copyOf(bits, bits.length);
      var word = bits0(hi) ^
          bit; // This will flip the bit off without changing the others.
      bits0[hi] = word;
      return new Mask(_trim(bits0), size - 1);
    } else {
      // The bit isn't set, so we're done!
      return this;
    }
  }

  Mask filter(bool f(int a)) {
    var bldr = new MaskBuilder();
    foreach((i) {
      if (f(i)) {
        bldr.add(i);
      }
    });
    return bldr.result();
  }

  Mask map(int f(int a)) {
    var bldr = new MaskBuilder();
    foreach((i) {
      bldr.add(f(i));
    });
    return bldr.result();
  }

  bool call(int n) {
//    var hi = n >>> 6; // The offset of the word containing bit n.
    var hi = (n & 0xFFFFFFFF) >> 6;
    if (hi < bits.length) {
      return (bits[hi] & (1 << (n & 0x3F))) != 0;
    } else {
      return false;
    }
  }

  Set<int> toSet() {
    var bldr = new Set<int>();
    foreach((b) {
      bldr.add(b);
    });
    return bldr;
  }

  /*BitSet toBitSet() {
    var bldr = BitSet.newBuilder();
    foreach((b) {
      bldr.add(b);
    });
    return bldr.result();
  }*/

  @override
  String toString() => "Mask(" + toSet().join(", ") + ")";

  @override
  bool operator ==(that) {
    if (that is Mask) {
      if (this.size != that.size) {
        return false;
      }
      // We bail early in the while loop, so if that.bits.length < this.bits.length,
      // then that.bits.length cannot be a prefix of this.bits.length, otherwise their
      // sizes would be different. So, i will never be out of bounds of either.
      var i = 0;
      while (i < bits.length) {
        var w0 = bits[i];
        var w1 = that.bits[i];
        if (w0 != w1) {
          return false;
        }
        i += 1;
      }
      return true;
    }
    return false;
  }

  @override
  int get hashCode => bits.fold(1914323553, (a, b) => a ^ b.hashCode);

  MaskBuilder newBuilder() => new MaskBuilder();

  /** An empty mask where all bits are unset. */
  static final empty = new Mask(new Uint32List(0), 0);

  /**
   * Returns a [[Mask]] where only the bits in `elems` are set to true and all
   * others are false.
   */
  factory Mask.from(Iterable<int> elems) {
    var bldr = new MaskBuilder();
    elems.forEach((e) {
      bldr.add(e);
    });
    return bldr.result();
  }

  /**
   * Returns a [[Mask]] with all bits from `from` to `until` (exclusive) set
   * and all others unset.
   */
  static Mask range(int from, int until) {
    var bldr = new MaskBuilder();
    var i = from;
    while (i < until) {
      bldr.add(i);
      i += 1;
    }
    return bldr.result();
  }

  /**
   * Create a Mask from an array of words representing the actual bit mask
   * itself. Please see [[Mask]] for a description of what `bits` should look
   * like.
   */
  static Mask fromBits(List<int> bits) {
    var i = 0;
    var size = 0;
    while (i < bits.length) {
      size += bitCount(bits[i]);
      i += 1;
    }
    return new Mask(_trim(bits), size);
  }

  // We need to ensure the highest order word is not 0, so we work backwards
  // and find the first, non-zero word, then trim the array so it becomes the
  // highest order word.
  static List<int> _trim(List<int> bits) {
    var i = bits.length;
    while (i > 0 && bits[i - 1] == 0) {
      i -= 1;
    }
    return (i == bits.length) ? bits : _copyOf(bits, i);
  }
}

class MaskBuilder {
  // bits.length may be larger than we need it, so `len` is the actual minimal
  // length required to store the bitset, with the highest order, non-zero word
  // at bits(len - 1).
  int len = 0;

  // The total number of 1 bits in the bitset.
  int size = 0;

  // The packed bitset.
  List<int> bits = new Uint32List(8);

  /**
   * Occasionally we have to enlarge the array if we haven't allocated enough
   * storage. We attempt to ~double the size of the current array.
   */
  void _resize(int newLen) {
    // Note: we won't ever require an array larger than 0x03FFFFFF, so we don't
    // need to worry about the length overflowing below.
    bits = Mask._copyOf(bits, highestOneBit(newLen) * 2);
    len = newLen;
  }

  void add(int n) {
//    var i = n >>> 6; // The offset of the word containing the bit n.
    var i = (n & 0xFFFFFFFF) >> 6;
    if (i >= bits.length) {
      _resize(i + 1);
    }
    if (i >= len) {
      len = i + 1;
    }
    var word = bits[i];
    var bit = 1 << (n & 0x3F);
    if ((word & bit) == 0) {
      // The bit isn't already set, so we add it and increase the size.
      bits[i] = word | bit;
      size += 1;
    }
  }

  Mask result() {
    var bits0 = Mask._copyOf(bits, len);
    return new Mask(bits0, size);
  }

  void clear() {
    len = 0;
    size = 0;
    bits = new Uint32List(8);
  }
}
