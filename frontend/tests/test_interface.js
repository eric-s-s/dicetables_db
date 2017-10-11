QUnit.test("onPageLoad first table not hidden, others are.", function (assert) {
    $.holdReady(true);
    onPageLoad();
    var allTables = $('.tableRequest');
    var shownTables = $('.tableRequest:visible');
    var hiddentables = $('.tableRequest:hidden');

    assert.equal(shownTables.length, 1, 'only one shown table');
    assert.equal(shownTables[0], allTables[0], 'the shown table is the first table');
    for (var mainIndex = 1; mainIndex < allTables.length; mainIndex++){
        assert.equal(allTables[mainIndex], hiddentables[mainIndex - 1]);
    }
});

QUnit.test("onPageLoad first statsForm not hidden, others are.", function (assert) {
    $.holdReady(true);
    onPageLoad();
    var allTables = $('.statsRequest');
    var shownStats = $('.statsRequest:visible');
    var hiddenStats = $('.statsRequest:hidden');

    assert.equal(shownStats.length, 1, 'only one shown statsForm');
    assert.equal(shownStats[0], allTables[0], 'the shown statsForm is the first one.');
    for (var mainIndex = 1; mainIndex < allTables.length; mainIndex++){
        assert.equal(allTables[mainIndex], hiddenStats[mainIndex - 1]);
    }
});

QUnit.test("onPageLoad first tableObj has data, all others 'data('tableObj') = null", function (assert) {
    $.holdReady(true);
    onPageLoad();
    $('.tableRequest').each(function(index){
        if (index === 0){
            assert.deepEqual($(this).data('tableObj'), fakeAnswer1);
        } else{
            assert.strictEqual($(this).data('tableObj'), null);
        }
    });
});

QUnit.test("onPageLoad first tableObj is graphed", function (assert) {
    $.holdReady(true);
    onPageLoad();
    var graphData = document.getElementById('plotter').data;
    assert.deepEqual(graphData[0].x, fakeAnswer1.data[0]);
    assert.deepEqual(graphData[0].y, fakeAnswer1.data[1]);
    assert.equal(graphData.length, 1);
});

QUnit.test("setUpHiddenForms hides all forms and stores id's in order as data", function (assert) {
    $.holdReady(true);
    var toPlaceIn = $('#answer');
    var placedClass = $('.tableRequest');
    placedClass.each(function () {
        $(this).show();
    });

    setUpHiddenForms(toPlaceIn, placedClass);
    assert.deepEqual(toPlaceIn.data('hiddenForms'), ['table-0', 'table-1', 'table-2']);
    placedClass.each(function () {
        assert.ok($(this).is(':hidden'));
    });

});

QUnit.test("showHiddenForm returns next idstr", function (assert) {
    $.holdReady(true);
    onPageLoad();
    var tableArea = $("#tableRequestArea");
    assert.deepEqual(tableArea.data('hiddenForms'), ['table-1', 'table-2']);
    assert.strictEqual(showHiddenForm(tableArea), 'table-1', 'returns next table');
    assert.strictEqual(showHiddenForm(tableArea), 'table-2', 'returns next table');
    assert.strictEqual(showHiddenForm(tableArea), null, 'out of tables, returns null');
    assert.strictEqual(showHiddenForm(tableArea), null, 'still out of tables, returns null');
});

QUnit.test("showHiddenForm shows next table", function (assert) {
    $.holdReady(true);
    onPageLoad();
    var tableArea = $("#tableRequestArea");
    var table1 = $("#table-1");
    var table2 = $("#table-2");

    assert.ok(table1.is(':hidden'), 'table1 starts hidden');
    assert.ok(table2.is(':hidden'), 'table2 starts hidden');

    showHiddenForm(tableArea);

    assert.ok(table1.is(':visible'), 'table1 visible');
    assert.ok(table2.is(':hidden'), 'table2 hidden');

    showHiddenForm(tableArea);

    assert.ok(table1.is(':visible'), 'table1 visible');
    assert.ok(table2.is(':visible'), 'table2 visible');
});


function initTest() {
    $.holdReady(true);
    $(".tableRequest").data('tableObj', null);
    $('#tableRequestArea').data('hiddenForms', []);
    $('#statsRequestArea').data('hiddenForms', []);
    Plotly.newPlot(document.getElementById('plotter'), [{x: [1], y: [1]}])
}

QUnit.test('plotCurrentTables no tables have data', function (assert) {
    initTest();

    plotCurrentTables();
    var graphData = document.getElementById('plotter').data;
    assert.ok(graphData === undefined || graphData.length === 0);
});

QUnit.test('plotCurrentTables removes data that is not a table', function (assert) {
    initTest();

    var graphDiv = document.getElementById('plotter');
    Plotly.newPlot(graphDiv, [{x:[1,2,3], y:[4, 5, 6]}]);
    assert.equal(graphDiv.data.length, 1, 'confirm it has graph data');

    plotCurrentTables();

    assert.equal(graphDiv.data.length, 0, 'confirm it no graph data');
});

QUnit.test('plotCurrentTables tables have data', function (assert) {
    initTest();

    $("#table-0").data('tableObj', fakeAnswer3);
    $('#table-1').data('tableObj', fakeAnswer1);
    plotCurrentTables();
    var graphData = document.getElementById('plotter').data;
    assert.equal(graphData.length, 2);
    assert.equal(graphData[0].x, fakeAnswer3.data[0]);
    assert.equal(graphData[0].y, fakeAnswer3.data[1]);
    assert.equal(graphData[1].x, fakeAnswer1.data[0]);
    assert.equal(graphData[1].y, fakeAnswer1.data[1]);
});

QUnit.test('plotCurrentTables tables gets new min and max', function (assert) {
    initTest();

    $("#table-0").data('tableObj', fakeAnswer3);
    $('#table-1').data('tableObj', fakeAnswer1);
    plotCurrentTables();
    assert.deepEqual(fakeAnswer3.range, [1, 16]);
    assert.deepEqual(fakeAnswer1.range, [3, 12]);

    $('.statsInput').each(function (){
        assert.equal(this.min, 1);
        assert.equal(this.max, 16);
    });
});

QUnit.test('getRangesForStats sets min/max/value to 0 if data is empty', function (assert) {
    initTest();
    plotCurrentTables(); // set up empty data.
    assert.equal(document.getElementById('plotter').data.length, 0);
    getRangesForStats();
    $('.statsInput').each(function() {
        assert.equal(this.value, 0);
        assert.equal(this.min, 0);
        assert.equal(this.max, 0);
    });
});

QUnit.test('getRangesForStats sets min/max/value to single x val if all data has same val.', function (assert) {
    initTest();
    var allStat = $('.statsInput');
    var graphDiv = document.getElementById('plotter');

    Plotly.newPlot(graphDiv, [{x:[1], y:[100]}, {x:[1], y:[-100]}, {x:[1], y:[-100]}]);
    getRangesForStats();
    allStat.each(function() {
        assert.equal(this.value, 1, 'positive number');
        assert.equal(this.min, 1, 'positive number');
        assert.equal(this.max, 1, 'positive number');
    });

    Plotly.newPlot(graphDiv, [{x:[-1], y:[100]}, {x:[-1], y:[-100]}, {x:[-1], y:[-100]}]);
    getRangesForStats();
    allStat.each(function() {
        assert.equal(this.value, -1, 'negative number');
        assert.equal(this.min, -1, 'negative number');
        assert.equal(this.max, -1, 'negative number');
    });
});

QUnit.test('getRangesForStats sets min/value to min x. max to max x', function (assert) {
    initTest();
    var allStat = $('.statsInput');
    var graphDiv = document.getElementById('plotter');

    Plotly.newPlot(graphDiv, [{x:[1, 2], y:[1, -1]}, {x:[2, 5], y:[-2, 2]}, {x:[3, 4], y:[-3, 3]}]);
    getRangesForStats();
    allStat.each(function() {
        assert.equal(this.value, 1, 'positive number');
        assert.equal(this.min, 1, 'positive number');
        assert.equal(this.max, 5, 'positive number');
    });

    Plotly.newPlot(graphDiv, [{x:[-1, -2], y:[1, -1]}, {x:[-2, -5], y:[-2, 2]}, {x:[-3, -4], y:[-3, 3]}]);
    getRangesForStats();
    allStat.each(function() {
        assert.equal(this.value, -5, 'negative number');
        assert.equal(this.min, -5, 'negative number');
        assert.equal(this.max, -1, 'negative number');
    });
});

QUnit.test('getTable assigns tableObj to table according to value', function (assert) {
    initTest();

    var table0 = $("#table-0");
    assert.equal(table0[0].tableQuery.value, 0);
    getTable(table0[0]);
    assert.deepEqual(table0.data('tableObj'), fakeAnswer1);

    table0[0].tableQuery.value = 2;
    getTable((table0[0]));
    assert.deepEqual(table0.data('tableObj'), fakeAnswer3);
});

QUnit.test('getTable plots current tables', function (assert) {
    initTest();

    var graphDiv = document.getElementById('plotter');

    getTable(document.getElementById('table-0'));

    assert.deepEqual(graphDiv.data[0].x, fakeAnswer1.data[0], 'one graph x vals');
    assert.deepEqual(graphDiv.data[0].y, fakeAnswer1.data[1], 'one graph y vals');
    assert.equal(graphDiv.data.length, 1, 'one graph data only one length');

    getTable(document.getElementById('table-1'));

    assert.deepEqual(graphDiv.data[0].x, fakeAnswer1.data[0], 'first graph x vals');
    assert.deepEqual(graphDiv.data[0].y, fakeAnswer1.data[1], 'first graph y vals');

    assert.deepEqual(graphDiv.data[1].x, fakeAnswer2.data[0], 'second graph x vals');
    assert.deepEqual(graphDiv.data[1].y, fakeAnswer2.data[1], 'second graph y vals');

    assert.equal(graphDiv.data.length, 2, 'data length 2');
});

QUnit.test('hideTableForm test all actions', function (assert) {
    initTest();
    var table0 = $('#table-0');
    var table1 = $('#table-1');

    getTable(table0[0]);
    getTable(table1[0]);

    var graphDiv = document.getElementById('plotter');

    hideTableForm('table-0');

    assert.ok(table0.is(':hidden'), 'tableForm is hidden');
    assert.strictEqual(table0.data('tableObj'), null, 'tableForm data set to null');
    assert.equal(graphDiv.data.length, 1, 'graphDiv doesn\'t contain the graph');
    assert.deepEqual(graphDiv.data[0].x, table1.data('tableObj').data[0], 'graphDiv info is table1 info: x');
    assert.deepEqual(graphDiv.data[0].y, table1.data('tableObj').data[1], 'graphDiv info is table1 info: y');
    assert.deepEqual($('#tableRequestArea').data('hiddenForms'), ['table-0'],
        'table put back into hiddenforms (testInit() makes "hiddenForms" an empty list)');

});

QUnit.test('removeStatsTraces no presence in data.', function (assert) {
    initTest();
    var graphDiv = document.getElementById('plotter');
    var beforeData = graphDiv.data;
    Plotly.newPlot(graphDiv, [{x:[2], y:[2]}]);
    var afterData = graphDiv.data;
    assert.notDeepEqual(beforeData, afterData, 'confirm that data can change');

    beforeData = graphDiv.data;
    removeStatsTraces('random');
    afterData = graphDiv.data;
    assert.deepEqual(beforeData, afterData);

});

QUnit.test('removeStatsTraces removes traces. relies on special "statsGroup" value', function (assert) {
    initTest();
    var group1 = [{x: [1], y: [1], name:'1', statsGroup: 'gp1'}, {x: [2], y: [2], name:'2', statsGroup: 'gp1'}];
    var group2 = [{x: [3], y: [3], name:'3', statsGroup: 'gp2'}, {x: [4], y: [4], name:'4', statsGroup: 'gp2'}];
    var groupNull = [{x: [5], y: [5], name:'5'}, {x: [6], y: [6], name:'6'}];

    var graphDiv = document.getElementById('plotter');
    Plotly.newPlot(graphDiv, group1);
    Plotly.addTraces(graphDiv, group2);
    Plotly.addTraces(graphDiv, groupNull);

    assert.equal(graphDiv.data.length, 6, 'setup complete');

    removeStatsTraces('gp2');
    var expectedNames = ['1', '2', '5', '6'];
    expectedNames.forEach(function (element, index) {
        assert.strictEqual(graphDiv.data[index].name, element, 'confirming remaining traces after removing "gp2"');
    });

    removeStatsTraces('gp1');
    expectedNames = ['5', '6'];
    expectedNames.forEach(function (element, index) {
        assert.strictEqual(graphDiv.data[index].name, element,
            'confirming remaining traces after removing "gp1" & "gp2"');
    });

});

QUnit.test('hideStatsForm all actions', function (assert) {
    initTest();
    var statsArea = $('#statsRequestArea');
    var stats0 = $('#stats-0');

    showHiddenForm(statsArea);

    var group1 = [{x: [3], y: [3], name:'3', statsGroup: 'stats-0'}, {x: [4], y: [4], name:'4', statsGroup: 'stats-0'}];
    var groupNull = [{x: [5], y: [5], name:'5'}, {x: [6], y: [6], name:'6'}];
    var graphDiv = document.getElementById('plotter');
    Plotly.newPlot(graphDiv, group1);
    Plotly.addTraces(graphDiv, groupNull);
    getRangesForStats();

    stats0[0].left.value = '6';
    stats0[0].right.value = '6';

    hideStatsForm('stats-0');

    assert.ok(stats0.is(":hidden"), 'statsForm is now hidden');

    assert.equal(stats0[0].left.min, 3, 'left min reset');
    assert.equal(stats0[0].left.max, 6, 'left max reset');
    assert.equal(stats0[0].left.value, 3, 'left value reset');
    assert.equal(stats0[0].right.min, 3, 'right min reset');
    assert.equal(stats0[0].right.max, 6, 'right max reset');
    assert.equal(stats0[0].right.value, 3, 'right value reset');

    assert.deepEqual(statsArea.data('hiddenForms'), ['stats-0'], 'stats back in hiddenForms');

    assert.equal(graphDiv.data.length, 2, 'graphDiv has correct number of graphs');
    assert.equal(graphDiv.data[0].name, '5', 'graphDiv has correct graphs');
    assert.equal(graphDiv.data[1].name, '6', 'graphDiv has correct graphs');


});

QUnit.test('getRange', function (assert) {
    assert.deepEqual(getRange('8', '9'), [8, 9], 'converts str to int');
    assert.deepEqual(getRange('8', '10'), [8, 9, 10], 'correctly sorts as ints');
    assert.deepEqual(getRange('10', '8'), [8, 9, 10], 'correctly sorts as ints other way');
    assert.deepEqual(getRange('1', '1'), [1], 'single number');
    assert.deepEqual(getRange('1', '-1'), [-1, 0, 1], 'positive to negative');
    assert.deepEqual(getRange('-1', '1'), [-1, 0, 1], 'negative to positive');
});

QUnit.test('statsGraphVals', function (assert) {
    var tableObj = {"repr": "<DiceTable containing [1D4  W:10]>",
        "data": [[1, 2, 3, 4], [10.0, 20.0, 50.0, 20.0]],
        "tableString": "1: 1\n2: 2\n3: 5\n4: 2\n",
        "forSciNum": {"1": ["1.00000", "0"], "2": ["2.00000", "0"], "3": ["5.00000", "0"], "4": ["2.00000", "0"]},
        "range": [1, 4],
        "mean": 2.8,
        "stddev": 0.8718};

    var expected = {x: [1, 2, 3, 4], y: [10.0, 20.0, 50.0, 20.0], type: 'scatter', mode: 'none', fill: 'tozeroy',
                    hoverinfo: 'skip'};

    var toTest = statsGraphVals([1, 2, 3, 4], tableObj);
    assert.deepEqual(toTest, expected, 'all x vals');

    expected.x = [1.52, 2, 3, 4];
    expected.y = [(10*0.48 + 20*0.52), 20.0, 50.0, 20.0];
    toTest = statsGraphVals([2, 3, 4], tableObj);
    assert.deepEqual(toTest, expected, 'query vals higher than min.');

    expected.x = [1, 2, 3, 3.48];
    expected.y = [10.0, 20.0, 50.0, (50 * 0.52 + 20.0 * 0.48)];
    toTest = statsGraphVals([1, 2, 3], tableObj);
    assert.deepEqual(toTest, expected, 'query vals lower than max.');

    expected.x = [2.52, 3, 3.48];
    expected.y = [(20 * 0.48 + 50 * 0.52), 50.0, (50 * 0.52 + 20.0 * 0.48)];
    toTest = statsGraphVals([3], tableObj);
    assert.deepEqual(toTest, expected, 'query vals singleton in middle.');

    expected.x = [1, 1.48];
    expected.y = [10.0, (10 * 0.52 + 20.0 * 0.48)];
    toTest = statsGraphVals([1], tableObj);
    assert.deepEqual(toTest, expected, 'query vals singleton at end.');


});

QUnit.test('statsGraphName tableObj has set name, pctString is any str and queryArr is in order', function (assert) {
    var tableObj = {'repr': '<DiceTable containing [1D4  W:3, 2D6]>'};
    assert.equal(statsGraphName(tableObj, '1.23e-10', [2]), '[1D4  W:3, 2D6]: [2]: 1.23e-10%',
        'single query. pct str in number range');
    assert.equal(statsGraphName(tableObj, '1.23e-1000', [2]), '[1D4  W:3, 2D6]: [2]: 1.23e-1000%',
        'single query. pct str not in number range');
    assert.equal(statsGraphName(tableObj, '1.23e-1000', [2, 3, 4]), '[1D4  W:3, 2D6]: [2to4]: 1.23e-1000%',
        'multi query.');
    assert.equal(statsGraphName(tableObj, '1.23e-1000', [-4, -3, -2]), '[1D4  W:3, 2D6]: [-4to-2]: 1.23e-1000%',
        'multi query with negative numbers.');
});

QUnit.test('statsGraphColor relies on the final digit of statsFormId and where graph is in index', function (assert) {
    assert.equal(statsGraphColor(0, 'randomStuff-1'), 'rgba(41,129,190,0.5)', 'index 0 ending in 1');
    assert.equal(statsGraphColor(1, 'randomStuff-9'), 'rgba(265,127,4,0.5)', 'index 1 ending in 9');
    var allDifferent = [];
    for (var endDigit = 0; endDigit < 10; endDigit++){allDifferent.push(statsGraphColor(0, 'words-' + endDigit));}
    for (var i=0; i < 10; i++){
        for (var j=i+1; j < 10; j++){
            assert.notEqual(statsGraphColor(0, 'words-' + i), statsGraphColor(0, 'words-' + j),
                'color vals different for different form IDs')
        }
    }
});

QUnit.test('plotStats no tables contain tableObj so no change to graph data.', function (assert) {
    initTest();
    var graphDiv = document.getElementById('plotter');
    var beforeData = graphDiv.data;
    var stats0 = document.getElementById('stats-0');
    stats0.left.value = 5;
    stats0.right.value = 10;

    var tableEntries = plotStats(stats0);
    var afterData = graphDiv.data;

    assert.deepEqual(beforeData, afterData);
    assert.equal(tableEntries.length, 0, 'no table entries.');
});

QUnit.test('plotStats', function (assert) {
    initTest();

    var graphDiv = document.getElementById('plotter');

    var table0 = $('#table-0');
    var table2 = $("#table-2");
    table0.data('tableObj', fakeAnswer1);
    table2.data('tableObj', fakeAnswer2);

    plotCurrentTables();
    assert.equal(graphDiv.data.length, 2, 'Setup has two traces in graph.');

    var stats0 = document.getElementById('stats-0');
    var stats1 = document.getElementById('stats-1');
    stats0.left.value = 5;
    stats0.right.value = 10;

    stats1.left.value = 12;
    stats1.right.value = 12;

    var tableEntry0 = plotStats(stats0);
    var expectedTableEntry = [
        {
            "header": "[3D4]",
            "occurrences": "56.00",
            "oneInChance": "1.143",
            "pctChance": "87.50",
            "total": "64.00"
        },
        {
            "header": "[3D6]",
            "occurrences": "104.0",
            "oneInChance": "2.077",
            "pctChance": "48.15",
            "total": "216.0"
        }
    ];

    assert.equal(graphDiv.data.length, 4, 'graphDiv now has four traces');
    assert.deepEqual(tableEntry0, expectedTableEntry, 'tableEntry ouput is correct');
    var expected3D4GraphData =
        {
            "fill": "tozeroy",
            "fillcolor": "rgba(31,109,190,0.5)",
            "hoverinfo": "skip",
            "legendgroup": "<DiceTable containing [3D4]>",
            "mode": "none",
            "name": "[3D4]: [5to10]: 87.50%",
            "statsGroup": "stats-0",
            "type": "scatter",
            "x": [
                4.52,
                5,
                6,
                7,
                8,
                9,
                10,
                10.48
            ],
            "y": [
                7.125,
                9.375,
                15.624999999999998,
                18.75,
                18.75,
                15.624999999999998,
                9.375,
                7.125
            ]
        };
    for (var key in expected3D4GraphData) {
        assert.deepEqual(graphDiv.data[2][key], expected3D4GraphData[key], 'all parts but uid are equal. 3D4')
    }

    var expected3D6GraphData =
        {
            "fill": "tozeroy",
            "fillcolor": "rgba(255,117,24,0.5)",
            "hoverinfo": "skip",
            "legendgroup": "<DiceTable containing [3D6]>",
            "mode": "none",
            "name": "[3D6]: [5to10]: 48.15%",
            "statsGroup": "stats-0",
            "type": "scatter",
            "x": [
                4.52,
                5,
                6,
                7,
                8,
                9,
                10,
                10.48
            ],
            "y": [
                2.111111111111111,
                2.7777777777777777,
                4.62962962962963,
                6.944444444444444,
                9.722222222222221,
                11.574074074074073,
                12.5,
                12.5
            ]
        };
    for (key in expected3D6GraphData) {
        assert.deepEqual(graphDiv.data[3][key], expected3D6GraphData[key], 'all parts but uid are equal. 3D6')
    }

    stats0.left.value = 6;
    stats0.right.value = 6;

    plotStats(stats0);

    assert.equal(graphDiv.data.length, 4, 'length did not change when regraphing same statsForm.');
    expected3D4GraphData = {
        "fill": "tozeroy",
        "fillcolor": "rgba(31,109,190,0.5)",
        "hoverinfo": "skip",
        "legendgroup": "<DiceTable containing [3D4]>",
        "mode": "none",
        "name": "[3D4]: [6]: 15.63%",
        "statsGroup": "stats-0",
        "type": "scatter",
        "x": [
            5.52,
            6,
            6.48
        ],
        "y": [
            12.625,
            15.624999999999998,
            17.125
        ]
    };
    for (key in expected3D4GraphData) {
        assert.deepEqual(graphDiv.data[2][key], expected3D4GraphData[key], 'new 3D4 graph is equal')
    }

    expected3D6GraphData = {
        "fill": "tozeroy",
        "fillcolor": "rgba(255,117,24,0.5)",
        "hoverinfo": "skip",
        "legendgroup": "<DiceTable containing [3D6]>",
        "mode": "none",
        "name": "[3D6]: [6]: 4.630%",
        "statsGroup": "stats-0",
        "type": "scatter",
        "x": [
            5.52,
            6,
            6.48
        ],
        "y": [
            3.7407407407407405,
            4.62962962962963,
            5.7407407407407405
        ]
    };
    for (key in expected3D6GraphData) {
        assert.deepEqual(graphDiv.data[3][key], expected3D6GraphData[key], 'new 3D6 graph is equal')
    }

    plotStats(stats1);
    assert.equal(graphDiv.data.length, 6, 'plotting new statsForm makes new traces.');

});

