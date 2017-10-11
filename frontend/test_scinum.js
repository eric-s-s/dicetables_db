
QUnit.test("new scinum positive", function (assert) {
    var toTestStrs = new SciNum("+1.23", "+456");
    assert.deepEqual(toTestStrs.mantissa, 1.23, "mantissa str ok");
    assert.deepEqual(toTestStrs.power, 456, "power str ok");
    var toTestNums = new SciNum(1.23, 456);
    assert.deepEqual(toTestNums.mantissa, 1.23, "mantissa float ok");
    assert.deepEqual(toTestNums.power, 456, "power int ok");
});

QUnit.test("new scinum negative", function (assert) {
    var toTestStrs = new SciNum("-1.23", "-456");
    assert.deepEqual(toTestStrs.mantissa, -1.23, "mantissa str ok");
    assert.deepEqual(toTestStrs.power, -456, "power str ok");
    var toTestNums = new SciNum(-1.23, -456);
    assert.deepEqual(toTestNums.mantissa, -1.23, "mantissa float ok");
    assert.deepEqual(toTestNums.power, -456, "power int ok");
});

QUnit.test("new SciNum zero value always has zero power", function (assert) {
    var zero = new SciNum(0, 100);
    assert.deepEqual(zero.power, 0, "power is zero");
    assert.deepEqual(zero, new SciNum(0, -5), "zero values evaluate to same");
});

QUnit.test("new SciNum +/-inf mantissa has zero power", function (assert) {
    var negInf = new SciNum(-Infinity, 100);
    assert.deepEqual(negInf.power, 0, "power is zero for neg inf");
    assert.deepEqual(negInf, new SciNum(-Infinity, -100), "neg inf all equal");

    var posInf = new SciNum(Infinity, 100);
    assert.deepEqual(posInf.power, 0, "power is zero for neg inf");
    assert.deepEqual(posInf, new SciNum(Infinity, -100), "neg inf all equal");

    assert.notDeepEqual(posInf, negInf, "pos and neg inf are not equal");

});

QUnit.test("div pos power", function (assert) {
    var big = new SciNum(2.00, 345);
    var small = new SciNum(4.00, 123);
    assert.deepEqual(big.div(small), new SciNum(5.00, 221), "big over small");
    assert.deepEqual(small.div(big), new SciNum(2, -222), "small over big");
    assert.deepEqual(small.div(small), new SciNum(1, 0), "small over small")
});

QUnit.test("div neg power", function (assert) {
    var big = new SciNum(-2.00, -345);
    var small = new SciNum(-4.00, -123);
    assert.deepEqual(big.div(small), new SciNum(5.00, -223), "big over small");
    assert.deepEqual(small.div(big), new SciNum(2, 222), "small over big");
    assert.deepEqual(small.div(small), new SciNum(1, 0), "small over small")
});

QUnit.test("mul pos power", function (assert) {
    var big = new SciNum(2.00, 345);
    var small = new SciNum(4.00, 123);
    assert.deepEqual(big.mul(small), new SciNum(8, 468), "big times small");
    assert.deepEqual(small.mul(big), new SciNum(8, 468), "small times big");
    assert.deepEqual(small.mul(small), new SciNum(1.6, 247), "small times small")
});

QUnit.test("mul neg power", function (assert) {
    var big = new SciNum(2.00, -345);
    var small = new SciNum(-4.00, -123);
    assert.deepEqual(big.mul(small), new SciNum(-8, -468), "big times small");
    assert.deepEqual(small.mul(big), new SciNum(-8, -468), "small times big");
    assert.deepEqual(small.mul(small), new SciNum(1.6, -245), "small times small")
});

QUnit.test("setting sigFigs", function (assert) {
    var tst = new SciNum(1, 2);
    assert.deepEqual(tst.sigFigs, 4, "start sigFigs");
    tst.sigFigs = 5;
    assert.deepEqual(tst.sigFigs, 5, "new sigFigs");
});

QUnit.test("toString no round", function (assert) {
    var tstPos = new SciNum(1.23, 456);
    var tstNeg = new SciNum(-1.23, -456);
    assert.deepEqual(tstPos.toString(), "1.23e+456", "pos mantissa, pos power");
    assert.deepEqual(tstNeg.toString(), "-1.23e-456", "neg mantissa, neg power");
});

QUnit.test("toNum neg pos infinity", function (assert) {
    assert.deepEqual(new SciNum(1, 350).toNum(), Infinity, "pos inf");
    assert.deepEqual(new SciNum(-1, 350).toNum(), -Infinity, "neg inf");
});

QUnit.test("toNum 0", function (assert) {
    assert.deepEqual(new SciNum(1, -350).toNum(), 0, "pos approach");
    assert.deepEqual(new SciNum(-1, -350).toNum(), 0, "neg approach");
    assert.deepEqual(new SciNum(0, 50).toNum(), 0, "zero mantissa");
});

QUnit.test("toNum neg powers", function (assert) {
    assert.deepEqual(new SciNum(1.2, -50).toNum(), 1.2e-50, "pos ");
    assert.deepEqual(new SciNum(-1.2, -50).toNum(), -1.2e-50, "neg ");
});

QUnit.test("toNum pos powers", function (assert) {
    assert.deepEqual(new SciNum(1.2, 50).toNum(), 1.2e50, "pos ");
    assert.deepEqual(new SciNum(-1.2, 50).toNum(), -1.2e50, "neg ");
});

QUnit.test('toFancyStr zero', function (assert) {
    assert.deepEqual(new SciNum(0, 100).toFancyStr(), '0.000');
    var changeSigFigs = new SciNum(0, 100);
    changeSigFigs.sigFigs = 5;
    assert.deepEqual(changeSigFigs.toFancyStr(), '0.0000');
});

QUnit.test('toFancyStr infinity', function (assert) {
    assert.deepEqual(new SciNum(Infinity, 0).toFancyStr(), '+\u221E', 'infinity \u221E');
    assert.deepEqual(new SciNum(-Infinity, 0).toFancyStr(), '-\u221E', 'neg inf \u221E');
});

QUnit.test("toFancyStr commaed", function (assert) {
    assert.deepEqual(new SciNum(1.23456, 5).toFancyStr(), "123,456");
    assert.deepEqual(new SciNum(1.23456, 3).toFancyStr(), "1,235");
    assert.deepEqual(new SciNum(1.23456, 2).toFancyStr(), "123.5");
});

QUnit.test("toFancyStr commaed but rounds up", function (assert) {
    assert.deepEqual(new SciNum(9.99999, 5).toFancyStr(), "999,999");
    assert.deepEqual(new SciNum(9.999999, 5).toFancyStr(), "1.000e+6");
    assert.deepEqual(new SciNum(9.99999, 3).toFancyStr(), "10,000");
    assert.deepEqual(new SciNum(9.99999, 2).toFancyStr(), "1,000");
    assert.deepEqual(new SciNum(9.99999, 1).toFancyStr(), "100.0");
    assert.deepEqual(new SciNum(9.99999, 0).toFancyStr(), "10.00");
    assert.deepEqual(new SciNum(9.99999, -1).toFancyStr(), "1.000");
});

QUnit.test("toFancyStr lt zero ", function (assert) {
    assert.deepEqual(new SciNum(1.23456, -1).toFancyStr(), "0.1235");
    assert.deepEqual(new SciNum(1.23456, -2).toFancyStr(), "0.01235");
    assert.deepEqual(new SciNum(1.23456, -3).toFancyStr(), "0.001235");
    assert.deepEqual(new SciNum(1.23456, -4).toFancyStr(), "1.235e-4");
});

QUnit.test("toFancyStr lt zero rounding", function (assert) {
    assert.deepEqual(new SciNum(9.99999, -1).toFancyStr(), "1.000");
    assert.deepEqual(new SciNum(9.99999, -2).toFancyStr(), "0.1000");
    assert.deepEqual(new SciNum(9.99999, -3).toFancyStr(), "0.01000");
    assert.deepEqual(new SciNum(9.99999, -4).toFancyStr(), "1.000e-3", " i'm a lazy bum and i won't fix this now.");
});

QUnit.test("toFancyStr large and small", function (assert) {
    assert.deepEqual(new SciNum(1.23456, -10).toFancyStr(), "1.235e-10");
    assert.deepEqual(new SciNum(1.23456, 10).toFancyStr(), "1.235e+10");
    assert.deepEqual(new SciNum(1.23456, -1000).toFancyStr(), "1.235e-1000");
    assert.deepEqual(new SciNum(1.23456, 1000).toFancyStr(), "1.235e+1000");
});

QUnit.test("toFancyStr large and small rounding", function (assert) {
    assert.deepEqual(new SciNum(9.99999, -10).toFancyStr(), "1.000e-9");
    assert.deepEqual(new SciNum(9.99999, 10).toFancyStr(), "1.000e+11");
    assert.deepEqual(new SciNum(9.99999, -1000).toFancyStr(), "1.000e-999");
    assert.deepEqual(new SciNum(9.99999, 1000).toFancyStr(), "1.000e+1001");
});

QUnit.test("toFancyStr large and small regression test", function (assert) {
    assert.deepEqual(new SciNum(9.1, -10).toFancyStr(), "9.100e-10");
    assert.deepEqual(new SciNum(9.1, 10).toFancyStr(), "9.100e+10");
    assert.deepEqual(new SciNum(9.1, -1000).toFancyStr(), "9.100e-1000");
    assert.deepEqual(new SciNum(9.1, 1000).toFancyStr(), "9.100e+1000");
});

function makeSNArray(numPairs) {
    return numPairs.map(function (el) { return new SciNum(el[0], el[1]); });
}

QUnit.test("helper test function makesSNArray", function (assert) {
    var expected = [new SciNum(1, 2), new SciNum(-3, -4)];
    assert.deepEqual(makeSNArray([[1, 2], [-3, -4]]), expected, "creates array");
});

QUnit.test("sumSciNum maxPower", function (assert) {
    assert.deepEqual(maxPower([]), -Infinity, "empty array not covered");
    assert.deepEqual(maxPower(makeSNArray([[1.1, 2]])), 2, "single element");
    assert.deepEqual(maxPower(makeSNArray([ [1.1, 2], [1.1, 30], [1.1, 20]])), 30, "multi elements");
    assert.deepEqual(maxPower(makeSNArray([ [1.1, -3], [1.1, -2], [1.1, -5] ])), -2, "neg elements");
});

QUnit.test("sumSciNum one element", function (assert) {
    var tstPos = new SciNum(1.23, 45);
    assert.deepEqual(sumSciNum([tstPos]), tstPos, "makes itself pos");
    var tstNeg = new SciNum(-1.23, -45);
    assert.deepEqual(sumSciNum([tstNeg]), tstNeg, "makes itself neg");
});

QUnit.test("sumSciNum multielements all same power", function (assert) {
    var power = 5;
    var notOverTen = makeSNArray([ [1.1, power], [2.2, power], [3.3, power] ]);
    assert.deepEqual(sumSciNum(notOverTen), new SciNum(6.6, power), "sum of mantissa lt ten");
    var overTen = makeSNArray([ [9.9, power], [9.9, power] ]);
    assert.deepEqual(sumSciNum(overTen), new SciNum(1.98, power + 1), "sum gt ten");
    var overHundred = makeSNArray([ [99, power], [99, power]]);
    assert.deepEqual(sumSciNum(overHundred), new SciNum(1.98, power + 2), "sum gt hundred");
});

QUnit.test("sumSciNum multielements all different power but not over maxPowerDiff", function (assert) {
    var notOverTen = makeSNArray([ [1.1, 4], [2.2, 3], [3.3, 2] ]);
    assert.deepEqual(sumSciNum(notOverTen), new SciNum(1.353, 4), "sum of mantissa lt ten");
    var overTen = makeSNArray([ [9.9, 4], [9.9, 3] ]);
    assert.deepEqual(sumSciNum(overTen), new SciNum(1.089, 5), "sum gt ten");
});

QUnit.test("sumSciNum multielements all different power over maxPowerDiff", function (assert) {
    var notOverTen = makeSNArray([ [1.1, 40], [2.2, 39], [3.3, 24] ]);
    assert.deepEqual(sumSciNum(notOverTen), new SciNum(1.32, 40), "sum of mantissa lt ten");
    var overTen = makeSNArray([ [9.9, 40], [9.9, 39] , [5, 24]]);
    assert.deepEqual(sumSciNum(overTen), new SciNum(1.089, 41), "sum gt ten");
    var onlyOneElCounted = makeSNArray([ [-1, 100], [2, 84], [3, 80], [4, -100]]);
    assert.deepEqual(sumSciNum(onlyOneElCounted), new SciNum(-1, 100), "only one element added");
});

QUnit.test("sumSciNum multielements negative powers negative values", function (assert) {
    var ltOnegtZero = makeSNArray([ [1, -40], [-2, -41], [3, -44] ]);
    assert.deepEqual(sumSciNum(ltOnegtZero), new SciNum(8.003, -41), "sum of mantissa lt one");
    var negNotOverTen = makeSNArray([ [-1, -1], [-2, -2] , [-3, -3]]);
    assert.deepEqual(sumSciNum(negNotOverTen), new SciNum(-1.23, -1), "sum not lt one");
    var negOverTen = makeSNArray([ [-9.9, -1], [-2, -2] , [-3, -3]]);
    assert.deepEqual(sumSciNum(negOverTen), new SciNum(-1.013, 0), "sum not lt one");
});

QUnit.test("sumSciNum sum of mantissa lt one.", function (assert) {
    var ltOnegtZero = makeSNArray( [ [0.1, 3] , [.02, 4]]);
    var answer = sumSciNum(ltOnegtZero);

    assert.deepEqual(answer.mantissa.toFixed(6), "3.000000", "float math means almost equal");
    assert.deepEqual(answer.power, 2)
});

QUnit.test("sumSciNum zero values", function (assert) {
    var zeroSum = makeSNArray( [ [0, 3] , [0, 4]]);
    var answer = sumSciNum(zeroSum);

    assert.deepEqual(answer, new SciNum(0, 0));
});

QUnit.test("sumSciNum empty arr", function (assert) {
    var answer = sumSciNum([]);
    assert.deepEqual(answer, new SciNum(0, 0));
});





