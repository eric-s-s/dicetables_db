
QUnit.test("makeSciNumArray single element", function (assert) {
    var posVals = [ ["+1", "+2"] ];
    assert.deepEqual(makeSciNumArray(posVals), [new SciNum(1, 2)], "single positive value");
    var negVals = [ ["-1", "-2"] ];
    assert.deepEqual(makeSciNumArray(negVals), [new SciNum(-1, -2)], "single positive value");
});

QUnit.test("makeSciNumArray multiple elements", function (assert) {
    var arrVals = [ ["+1", "+2"] , ["3", "-4"], ["-5.2", "67"], ["-8.9", "-1000"]];
    assert.deepEqual(
        makeSciNumArray(arrVals),
        [new SciNum(1, 2), new SciNum(3, -4), new SciNum(-5.2, 67), new SciNum(-8.9, -1000)],
        "multiple elements"
    );
});

QUnit.test("createStatsObj positive poswers commaed", function (assert) {
    var arrVals = makeSciNumArray([ [1, 4] , [2, 3]]);
    var total = new SciNum(2.4, 5);
    var expected = {total: "240,000", occurrences: "12,000", oneInChance: "20.00", pctChance: "5.000"};
    assert.deepEqual(
        createStatsObj(total, arrVals),
        expected,
        "all answers commaed notation"
    );
});

QUnit.test("createStatsObj large numbers large chance", function (assert) {
    var arrVals = makeSciNumArray([ [1, 0] , [1, 100]]);
    var total = new SciNum(2.0000, 100);
    var expected = {total: "2.000e+100", occurrences: "1.000e+100", oneInChance: "2.000", pctChance: "50.00"};
    assert.deepEqual(
        createStatsObj(total, arrVals),
        expected,
        "one element larger than others"
    );
});

QUnit.test("createStatsObj large numbers small chance", function (assert) {
    var arrVals = makeSciNumArray([[4, 100], [1, 100]]);
    var total = new SciNum(2.0000, 10000);
    var expected = {
        total: "2.000e+10000", occurrences: "5.000e+100",
        oneInChance: "4.000e+9899", pctChance: "2.500e-9898"
    };
    assert.deepEqual(
        createStatsObj(total, arrVals),
        expected,
        "total is much bigger."
    );
});

QUnit.test("createStatsObj very small numbers", function (assert) {
    var total = new SciNum(1.000, -500);
    var arrVals = makeSciNumArray([ [5.000, -502], [2.000, -501]]);
    var expected = {
        total: "1.000e-500", occurrences: "2.500e-501",
        oneInChance: "4.000", pctChance: "25.00"
    };
    assert.deepEqual(
        createStatsObj(total, arrVals),
        expected
    );
});

QUnit.test("createStatsObj low positive powers", function (assert) {
    var total = new SciNum(1.000, 2);
    var arrVals = makeSciNumArray([ [5.0, 0], [1.5, 1]]);
    var expected = {
        total: "100.0", occurrences: "20.00",
        oneInChance: "5.000", pctChance: "20.00"
    };
    assert.deepEqual(
        createStatsObj(total, arrVals),
        expected
    );
});

QUnit.test("createStatsObj zero value", function (assert) {
    var total = new SciNum(1.000, 2);
    var arrVals = makeSciNumArray([ [0, 0], [0, 1]]);
    var expected = {
        total: "100.0", occurrences: "0.000",
        oneInChance: "+\u221E", pctChance: "0.000"
    };
    assert.deepEqual(
        createStatsObj(total, arrVals),
        expected
    );
});

QUnit.test("createSciNumObj", function (assert) {
    var inputObj = {'1': ['1.23', '10'], '2': ['1.23', '11']};
    var expected = {'1': new SciNum('1.23', '10'), 2: new SciNum('1.23', '11'),
        'total': new SciNum(1.353, 11)};
    assert.deepEqual(createSciNumObj(inputObj), expected);

});

QUnit.test("createSciNumObj values are nums", function (assert) {
    var inputObj = {1: [1.23, 10], 2: [1.23, 11]};
    var expected = {'1': new SciNum('1.23', '10'), 2: new SciNum('1.23', '11'),
        'total': new SciNum(1.353, 11)};
    assert.deepEqual(createSciNumObj(inputObj), expected);

});

QUnit.test("getSciNumValue obj has value", function (assert) {
    var toTest = new SciNum(1, 2);
    var objUsingNum = {1: toTest};
    var objUsingStr = {"1": toTest};
    assert.deepEqual(getSciNumValue(objUsingNum, 1), toTest, "num key called with num");
    assert.deepEqual(getSciNumValue(objUsingNum, '1'), toTest, "num key called with str");
    assert.deepEqual(getSciNumValue(objUsingStr, 1), toTest, "str key called with num");
    assert.deepEqual(getSciNumValue(objUsingStr, '1'), toTest, "str key called with str");

});

QUnit.test("getSciNumValue obj doesn't have value", function (assert) {
    var toTest = new SciNum(1, 2);
    var zeroSciNum = new SciNum(0, 0);
    var objUsingNum = {1: toTest};
    var objUsingStr = {"1": toTest};
    assert.deepEqual(getSciNumValue(objUsingNum, 2), zeroSciNum, "num key called with num");
    assert.deepEqual(getSciNumValue(objUsingNum, '2'), zeroSciNum, "num key called with str");
    assert.deepEqual(getSciNumValue(objUsingStr, 2), zeroSciNum, "str key called with num");
    assert.deepEqual(getSciNumValue(objUsingStr, '2'), zeroSciNum, "str key called with str");

});

QUnit.test("getStats arr is all keys in obj", function (assert) {
    var sciNumObj = {1: new SciNum(1, 1), 2: new SciNum(2, 1), 3: new SciNum(2, 1), total: new SciNum(5, 1)};
    var expected = {
        total: "50.00", occurrences: "50.00",
        oneInChance: "1.000", pctChance: "100.0"
    };
    assert.deepEqual(getStats(sciNumObj, ['1', '2', '3']), expected, "with str array");
    assert.deepEqual(getStats(sciNumObj, [1, 2, 3]), expected, "with num array");
});

QUnit.test("getStats arr is no keys in obj", function (assert) {
    var sciNumObj = {1: new SciNum(1, 1), 2: new SciNum(2, 1), 3: new SciNum(2, 1), total: new SciNum(5, 1)};
    var expected = {
        total: "50.00", occurrences: "0.000",
        oneInChance: "+\u221E", pctChance: "0.000"
    };
    assert.deepEqual(getStats(sciNumObj, [5 , 6, 7]), expected, "with str array");
    assert.deepEqual(getStats(sciNumObj, ['a', 'b']), expected, "with num array");
});

QUnit.test("getStats arr is empty", function (assert) {
    var sciNumObj = {1: new SciNum(1, 1), 2: new SciNum(2, 1), 3: new SciNum(2, 1), total: new SciNum(5, 1)};
    var expected = {
        total: "50.00", occurrences: "0.000",
        oneInChance: "+\u221E", pctChance: "0.000"
    };
    assert.deepEqual(getStats(sciNumObj, []), expected);
});

QUnit.test("getStats arr is some keys in obj.", function (assert) {
    var sciNumObj = {1: new SciNum(1, 1), 2: new SciNum(2, 1), 3: new SciNum(2, 1), total: new SciNum(5, 1)};
    var expected = {
        total: "50.00", occurrences: "30.00",
        oneInChance: "1.667", pctChance: "60.00"
    };
    assert.deepEqual(getStats(sciNumObj, [0, 1, 2]), expected, "with str array");
});

QUnit.test("getStats large numbers large occurrences", function (assert) {
    var sciNumObj = {1: new SciNum(1, 1000), 2: new SciNum(2, 1000), 3: new SciNum(2, 1000),
                     total: new SciNum(5, 1000)};
    var expected = {
        total: "5.000e+1000", occurrences: "3.000e+1000",
        oneInChance: "1.667", pctChance: "60.00"
    };
    assert.deepEqual(getStats(sciNumObj, [0, 1, 2]), expected);
});

QUnit.test("getStats large numbers small occurrences", function (assert) {
    var sciNumObj = {1: new SciNum(1, 1), 2: new SciNum(2, 1), 3: new SciNum(2, 1000),
                     total: new SciNum(2, 1000)};
    var expected = {
        total: "2.000e+1000", occurrences: "30.00",
        oneInChance: "6.667e+998", pctChance: "1.500e-997"
    };
    assert.deepEqual(getStats(sciNumObj, [0, 1, 2]), expected);
});

