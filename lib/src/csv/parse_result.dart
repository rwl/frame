part of frame.csv;

abstract class Instr<A> {}

abstract class ParseResult<A> implements Instr<A> {}

class Emit<A> implements ParseResult<A> {
  final A value;
  Emit(this.value);
}

class Fail implements ParseResult<Nothing> {
  final String message;
  final int pos;
  Fail(this.message, this.pos);
}

const _NeedInput NeedInput = const _NeedInput();

class _NeedInput implements ParseResult<Nothing> {
  const _NeedInput();
}

const _Resume Resume = const _Resume();

class _Resume implements Instr<Nothing> {
  const _Resume();
}

const Done = const _Done();

class _Done implements Instr<Nothing> {
  const _Done();
}
