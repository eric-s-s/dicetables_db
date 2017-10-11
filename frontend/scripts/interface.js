
$(onPageLoad);

function onPageLoad() {
    var allTableForms = $('.tableRequest');
    var allStatsForms = $('.statsRequest');
    var tableRequestArea = $('#tableRequestArea');
    var statRequestArea = $('#statsRequestArea');

    allTableForms.submit(function (event) {
        event.preventDefault();
        getTable(this);
    });
    allTableForms.data('tableObj', null);

    allStatsForms.submit(function (event) {
        event.preventDefault();
        plotStats(this);
    });

    setUpHiddenForms(statRequestArea, allStatsForms);
    setUpHiddenForms(tableRequestArea, allTableForms);

    var idStr = showHiddenForm(tableRequestArea);
    getTable(document.getElementById(idStr));

    showHiddenForm(statRequestArea);

    $('#more').click(function () {showHiddenForm(tableRequestArea);});
    $('#moreStats').click(function () {showHiddenForm(statRequestArea);});

    $('.rmStats').click(function () {hideStatsForm(this.parentNode.id);});

    $('.rmTable').click(function () {hideTableForm(this.parentNode.id);});

    $("#basic").text('stddev: ' + fakeAnswer1.stddev + '\nmean: ' + fakeAnswer1.mean + '\nrange: ' + fakeAnswer1.range);
}

function setUpHiddenForms(containerJQuery, classJQuery) {
    var hiddenForms = [];
    classJQuery.each(function () {
        $(this).hide();
        hiddenForms.push(this.id);
    });
    hiddenForms.sort();
    containerJQuery.data('hiddenForms', hiddenForms);
}

function getTable(tableForm) {
    var index = tableForm.tableQuery.value % fakeList.length;
    $('#' + tableForm.id).data('tableObj', fakeList[index]);
    plotCurrentTables();
}

function hideTableForm(idStr) {
    var theForm = $('#' + idStr);
    theForm.hide();
    theForm.data('tableObj', null);
    theForm[0].reset();
    var hiddenForms = $('#tableRequestArea').data('hiddenForms');
    hiddenForms.push(idStr);
    hiddenForms.sort();
    plotCurrentTables();
}

function hideStatsForm(idStr) {
    var theForm = $('#' + idStr);
    theForm.hide();
    theForm[0].reset();
    var hiddenForms = $('#statsRequestArea').data('hiddenForms');
    hiddenForms.push(idStr);
    hiddenForms.sort();
    removeStatsTraces(idStr);
}

function showHiddenForm(requestAreaJQuery) {
    var hiddentTables = requestAreaJQuery.data('hiddenForms');
    if (hiddentTables.length > 0) {
        var idStr = hiddentTables.shift();
        $('#' + idStr).show();
        return idStr;
    }
    return null;
}

function plotCurrentTables () {
    var plotData = [];
    $('.tableRequest').each( function () {
        var tableObj = $('#' + this.id).data('tableObj');
        if (tableObj !== null) {
            var datum = {
                x: tableObj.data[0],
                y: tableObj.data[1],
                name: getDiceListString(tableObj.repr)
            };
            plotData.push(datum);
        }
    });
    var graphDiv = document.getElementById('plotter');
    Plotly.newPlot(graphDiv, plotData, {margin: {t: 1}});
    getRangesForStats();
}

function getRangesForStats() {
    var data = document.getElementById('plotter').data;
    var min = Infinity;
    var max = -Infinity;
    data.forEach(function (el) {
        var xVals = el.x;
        var elMin = Math.min.apply(null, xVals);
        var elMax = Math.max.apply(null, xVals);
        min = Math.min(min, elMin);
        max = Math.max(max, elMax);
    });
    if (min === Infinity || max === -Infinity) {min = 0; max = 0;}
    $('.statsInput').attr({'min': min, 'value': min, 'max': max});
}

function plotStats(statsForm) {
    removeStatsTraces(statsForm.id);
    var graphDiv = document.getElementById('plotter');

    var queryArr = getRange(statsForm.left.value, statsForm.right.value);

    var statsData = [];
    var tableEntries = [];
    var nonNullDataIndex = 0;

    $('.tableRequest').each(function () {
        var tableObj = $('#' + this.id).data('tableObj');
        if (tableObj !== null) {

            var forStats = createSciNumObj(tableObj.forSciNum);
            var statsInfo = getStats(forStats, queryArr);
            statsInfo['header'] = getDiceListString(tableObj.repr);

            var traceDatum = statsGraphVals(queryArr, tableObj);
            traceDatum['name'] = statsGraphName(tableObj, statsInfo.pctChance, queryArr);

            traceDatum['fillcolor'] = statsGraphColor(nonNullDataIndex, statsForm.id);
            traceDatum['statsGroup'] = statsForm.id;
            traceDatum['legendgroup'] = tableObj.repr;
            nonNullDataIndex++;

            statsData.push(traceDatum);
            tableEntries.push(statsInfo);

        }
    });

    Plotly.addTraces(graphDiv, statsData);
    return tableEntries;
}


function removeStatsTraces(statsFormId) {
    var graphDiv = document.getElementById('plotter');
    var toRemove = [];
    for (var i = 0; i < graphDiv.data.length; i++) {
        if (graphDiv.data[i].statsGroup === statsFormId) {
            toRemove.push(i);
        }
    }
    Plotly.deleteTraces(graphDiv, toRemove);
}


function getRange (left, right) {
    var leftInt = parseInt(left);
    var rightInt = parseInt(right);
    var out = [];
    var stop, start;
    if (leftInt < rightInt) {
        start = leftInt;
        stop = rightInt;
    } else {
        start = rightInt;
        stop = leftInt;
    }
    for (var i = start; i <= stop; i++){
        out.push(i);
    }
    return out;
}


function statsGraphVals(queryArr, tableObj) {
    var start = Math.max(queryArr[0], tableObj.range[0]);
    var stop = Math.min(queryArr[queryArr.length - 1], tableObj.range[1]);
    var startIndex = tableObj.data[0].indexOf(start);
    var stopIndex = tableObj.data[0].indexOf(stop);
    var xVals = tableObj.data[0].slice(startIndex, stopIndex + 1);
    var yVals = tableObj.data[1].slice(startIndex, stopIndex + 1);
    if (start > tableObj.range[0]) {
        var beforeVal = 0.48 * tableObj.data[1][startIndex - 1] + 0.52 * tableObj.data[1][startIndex];
        xVals.unshift(start - 0.48);
        yVals.unshift(beforeVal);
    }
    if (stop < tableObj.range[1]) {
        var afterVal = 0.48 * tableObj.data[1][stopIndex + 1] + 0.52 * tableObj.data[1][stopIndex];
        xVals.push(stop + 0.48);
        yVals.push(afterVal);
    }
    return {x: xVals, y: yVals, type: 'scatter', mode: 'none', fill: 'tozeroy', hoverinfo:'skip'};
}

function statsGraphName(tableObj, pctString, queryArr) {
    var tableName = getDiceListString(tableObj.repr);
    var query = (queryArr.length === 1) ? queryArr[0]: queryArr[0] + 'to' + queryArr[queryArr.length - 1];
    return tableName + ': [' + query + ']: ' + pctString + '%';
}

function getDiceListString(diceTableRepr) {
    return diceTableRepr.slice("<DiceTable containing ".length, -1);
}

function statsGraphColor(matchGraphIndex, statsFormId) {
    var colorObjs = [
        {'r': 31, 'g': 119, 'b': 180, 'a': 0.5},
        {'r': 255, 'g': 127, 'b': 14, 'a': 0.5},
        {'r': 44, 'g': 160, 'b': 44, 'a': 0.5},
        {'r': 214, 'g': 39, 'b': 40, 'a': 0.5},
        {'r': 148, 'g': 103, 'b': 189, 'a': 0.5},
        {'r': 140, 'g': 86, 'b': 75, 'a': 0.5},
        {'r': 227, 'g': 119, 'b': 194, 'a': 0.5},
        {'r': 127, 'g': 127, 'b': 127, 'a': 0.5},
        {'r': 188, 'g': 189, 'b': 34, 'a': 0.5},
        {'r': 23, 'g': 190, 'b': 207, 'a': 0.5}
    ];
    var rgbaObj = colorObjs[matchGraphIndex];
    var modValues = [
        [0, -10, 10], [10, 10, 10], [-10, -10, -10], [-10, 10, -10], [10, -10, 10],
        [-10, -10, 10], [10, -10, -10], [-10, 10, 10], [10, 10, -10], [10, 0, -10]
    ];
    var mod = modValues[statsFormId.slice(-1)];

    return 'rgba(' + (rgbaObj.r + mod[0]) + ',' + (rgbaObj.g + mod[1]) + ',' + (rgbaObj.b + mod[2]) +',0.5)';
}
