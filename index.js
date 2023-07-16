// Custom Aggregators

(function () {
    var callWithJQuery,
        indexOf = [].indexOf || function (item) {
            for (var i = 0, l = this.length; i < l; i++) {
                if (i in this && this[i] === item) return i;
            }
            return -1;
        },
        slice = [].slice,
        bind = function (fn, me) {
            return function () {
                return fn.apply(me, arguments);
            };
        },
        hasProp = {}.hasOwnProperty;;

    callWithJQuery = function (pivotModule) {
        if (typeof exports === "object" && typeof module === "object") {
            return pivotModule(require("jquery"));
        } else if (typeof define === "function" && define.amd) {
            return define(["jquery"], pivotModule);
        } else {
            return pivotModule(jQuery);
        }
    };

    callWithJQuery(function ($) {


        var multifactAggregator = function (aggMap, derivedAggregations) {

            var allAggregators = $.map(aggMap, function (aggregation, key) {
                var agg = $.pivotUtilities.aggregators[aggregation.aggType];
//                console.log({a:agg})
                var _numInputs = agg([])().numInputs || 0;
                return {
                    aggregator: agg,
                    selfNumInputs: _numInputs,
                    name: aggregation.name,
                    key: key,
                    varName: aggregation.varName,
                    hidden: aggregation.hidden,
                    format: aggregation.format 
                }
            });

            return function (facts) {
                //console.log('multifact sum agg called with ', facts);


                var aggregations = $.map(allAggregators, function (_agg) {

                    //console.log(aggMap[_agg.name].arguments);
                    return {
                        aggregator: _agg.aggregator(aggMap[_agg.key].arguments),
                        key: _agg.key,
                        name: _agg.name,
                        varName: _agg.varName,
                        hidden: _agg.hidden,
                        format: _agg.format
                    };

                })


                return function (data, rowKey, colKey) {


                    var finalAggregators = $.map(aggregations, function (_agg) {
                        return {
                            aggregator: _agg.aggregator(data, rowKey, colKey),
                            key: _agg.key,
                            name: _agg.name,
                            varName: _agg.varName,
                            hidden: _agg.hidden,
                            format: _agg.format
                        };

                    });

                    var _finalAggregatorsNameMap = {};
                    for (var i = 0, x = finalAggregators.length; i < x; i++) {
                        var aggregation = finalAggregators[i];
                        _finalAggregatorsNameMap[aggregation.name] = aggregation;

                    }

                    var _finalDerivedAggregatorsNameMap = {};
                    for (var i = 0, x = derivedAggregations.length; i < x; i++) {
                        var derivedAggregation = derivedAggregations[i];
                        _finalDerivedAggregatorsNameMap[derivedAggregation.name] = derivedAggregation;
                    }

                    //console.log(finalAggregators);

                    if (!facts && !!data && !!data.valAttrs) {
                        facts = data.valAttrs;
                    }
                    //console.log('aggregator called for', data, rowKey, colKey)

                    var analytics = {};


                    return {
                        label: "Facts",

                        push: function (record) {

                            for (var i = 0, x = finalAggregators.length; i < x; i++) {
                                var aggregation = finalAggregators[i];
                                //console.log('Final Aggregation', aggregation)
                                aggregation.aggregator.push(record);

                            }

                        },
                        inner : {
                                value : function(){
                                    try{
                                        //extract which stat is this being called for
                                        var stat = arguments.callee.caller.arguments[0];
                                        //Get the aggregator for this stat
                                        var aggregator = _finalAggregatorsNameMap[stat];

                                        return aggregator.aggregator.inner.value();
                                    } catch(e){
                                        return -100;

                                    }

                                }

                            }

                        ,

                        multivalue: function () {

                            analytics = {};
                            var variables = {};

                            var finalAnalytics = {};


                            for (var i = 0, x = finalAggregators.length; i < x; i++) {
                                var aggregation = finalAggregators[i];
                                analytics[aggregation.name] = aggregation.aggregator.value(aggregation.name);
                                variables[aggregation.varName] = analytics[aggregation.name];
                                if (!aggregation.hidden) {
                                    finalAnalytics[aggregation.name] = analytics[aggregation.name];
                                }

                            }

                            //console.log(analytics)

                            //console.log(variables);


                            var _derivedAnalytics = {};
                            for (var i = 0, x = derivedAggregations.length; i < x; i++) {
                                var derivedAggregation = derivedAggregations[i];
                                var derivedVal = 0;
                                var expression = 'derivedVal = ' + derivedAggregation.expression;
                                eval(expression);
                                _derivedAnalytics[derivedAggregation.name] = derivedVal;
                            }
                            //console.log(_derivedAnalytics);
                            finalAnalytics = $.extend(finalAnalytics, _derivedAnalytics)

                            return finalAnalytics;
                        },

                        // return the first element for unsupported renderers.
                        value: function () {
                            return 'Multi-Fact-Aggregator does not support single value';
                        },
                        format: function (x, aggKey) {
                            //console.log('i for formatter',i);
                            var formatter = null;
                            //console.log(_finalAggregatorsNameMap,aggKey)
                            if (!!_finalAggregatorsNameMap[aggKey]) {                               
                                formatter = _finalAggregatorsNameMap[aggKey].format;
                            } else if (!!_finalDerivedAggregatorsNameMap[aggKey]) {
                                var formatterOptions = $.extend({}, _finalDerivedAggregatorsNameMap[aggKey].formatterOptions);

                                formatter = $.pivotUtilities.numberFormat(formatterOptions);
                            }

                            if (!formatter) {
                                formatter = $.pivotUtilities.numberFormat();
                            }
                            //console.log(formatter)
                            return formatter(x);


                        }


                    };
                };
            };
        }


        $.pivotUtilities.multifactAggregatorGenerator = multifactAggregator;


        $.fn.gtBarchart = function(opts) {
            var barcharter, i, l, numCols, numRows, ref;
            numRows = this.data("numrows");
            numCols = this.data("numcols");

            var _finalAggregatorsNameMap = {};
            var finalAggregators = $.map(opts.aggregations.defaultAggregations, function (aggregation) {
                return aggregation;
            }) || [];
            for (i = 0, x = finalAggregators.length; i < x; i++) {
                var aggregation = finalAggregators[i];
                _finalAggregatorsNameMap[aggregation.name] = aggregation;

            }

            var derivedAggregations = opts.aggregations.derivedAggregations || [];
            var _finalDerivedAggregatorsNameMap = {};
            for (i = 0, x = derivedAggregations.length; i < x; i++) {
                var derivedAggregation = derivedAggregations[i];
                _finalDerivedAggregatorsNameMap[derivedAggregation.name] = derivedAggregation;
            }

            var aggregationMap = $.extend(true, {}, _finalAggregatorsNameMap, _finalDerivedAggregatorsNameMap)


            barcharter = (function(_this) {
                return function(scope) {
                    var forEachCell, max, min, range, values, valueSets;
                    forEachCell = function(f) {
                        return _this.find(scope).each(function() {
                            var x, valueAttributeKey;
                            x = $(this).data("value");
                            valueAttributeKey = $(this).data("value-for");
                            if ((x != null) && isFinite(x)) {
                                return f(x, $(this), valueAttributeKey);
                            }
                        });
                    };
                    values = [];
                    valueSets = {};
                    forEachCell(function(x, elem, valueAttributeKey) {
                        valueSets[valueAttributeKey] = valueSets[valueAttributeKey] || [];
                        valueSets[valueAttributeKey].push(x);
                        return values.push(x);
                    });

                    var scalerSet = {};

                    Object.keys(valueSets).forEach(function(key){
                        if(valueSets.hasOwnProperty(key)){

                            max = Math.max.apply(Math, values);
                            if (max < 0) {
                                max = 0;
                            }

                            range = max;

                            min = Math.min.apply(Math, values);
                            if (min < 0) {
                                range = max - min;
                            }
                            scalerSet[key] = (function(){ return function(x) {
                                return 100 * x / (1.4 * range);
                            }})();
                        }
                    });


                    return forEachCell(function(x, elem, valueAttributeKey) {
                        var bBase, bgColor, text, wrapper, scaler;
                        scaler = scalerSet[valueAttributeKey];

                        var opts = aggregationMap[valueAttributeKey];

                        if(opts && opts.renderEnhancement ==='barchart'){
                            text = elem.text();
                            wrapper = $("<div>").css({
                                "position": "relative",
                                "width": "120px"
                            });
                            bgColor = opts.barchartColor || "steelblue";
                            bBase = 0;
                            if (min < 0) {
                                bBase = scaler(-min);
                            }
                            if (x < 0) {
                                bBase += scaler(x);
                                bgColor = opts.barchartNegativeColor || "darkred";
                                x = -x;
                            }
                            wrapper.append($("<div>").css({
                                "position": "absolute",
                                "top": bBase + "%",
                                "left": 0,
                                //"right": 0,
                                "height" : "12px",
                                "width": scaler(x) + "%",
                                "background-color": bgColor
                            }));
                            wrapper.append($("<div>").text(text).css({
                                "position": "relative",
                                "padding-left": "5px",
                                "padding-right": "5px"
                            }));
                            return elem.css({
                                "padding": 0,
                                "padding-top": "5px",
                                "text-align": "right"
                            }).html(wrapper);
                        } else {
                            return elem;
                        }

                    });
                };
            })(this);
            for (i = l = 0, ref = numRows; 0 <= ref ? l < ref : l > ref; i = 0 <= ref ? ++l : --l) {
                barcharter(".pvtVal.row" + i);
            }
            //barcharter(".pvtTotal.colTotal");
            return this;
        };

        $.fn.gtHeatmap = function (scope, opts) {
            var colorScaleGenerator, heatmapper, i, x, j, l, n, numCols, numRows, ref, ref1, ref2, _finalAggregatorsNameMap, _finalDerivedAggregatorsNameMap;
            if (scope == null) {
                scope = "heatmap";
            }
            numRows = this.data("numrows");
            numCols = this.data("numcols");


            _finalAggregatorsNameMap = {};
            var finalAggregators = $.map(opts.aggregations.defaultAggregations, function (aggregation) {
                return aggregation;
            }) || [];
            for (i = 0, x = finalAggregators.length; i < x; i++) {
                var aggregation = finalAggregators[i];
                _finalAggregatorsNameMap[aggregation.name] = aggregation;

            }

            var derivedAggregations = opts.aggregations.derivedAggregations || [];
            _finalDerivedAggregatorsNameMap = {};
            for (i = 0, x = derivedAggregations.length; i < x; i++) {
                var derivedAggregation = derivedAggregations[i];
                _finalDerivedAggregatorsNameMap[derivedAggregation.name] = derivedAggregation;
            }

            var aggregationMap = $.extend(true, {}, _finalAggregatorsNameMap, _finalDerivedAggregatorsNameMap)


            colorScaleGenerator = opts != null ? (ref = opts.heatmap) != null ? ref.colorScaleGenerator : void 0 : void 0;
            if (colorScaleGenerator == null) {
                colorScaleGenerator = function (values) {
                    var max, min;
                    min = Math.min.apply(Math, values);
                    max = Math.max.apply(Math, values);
                    return function (x) {
                        var nonRed;
                        nonRed = 255 - Math.round(255 * (x - min) / (max - min));
                        return "rgb(255," + nonRed + "," + nonRed + ")";
                    };
                };
            }
            heatmapper = (function (_this) {
                return function (scope) {
                    var colorScale, forEachCell, values, valueSets, colorScaleSets;
                    forEachCell = function (f) {
                        return _this.find(scope).each(function () {
                            var x, valueAttributeKey;
                            x = $(this).data("value");
                            valueAttributeKey = $(this).data("value-for");
                            if ((x != null) && isFinite(x)) {
                                return f(x, $(this), valueAttributeKey);
                            }
                        });
                    };
                    values = [];
                    valueSets = {};
                    forEachCell(function (x, elem, valueAttributeKey) {
                        valueSets[valueAttributeKey] = valueSets[valueAttributeKey] || [];
                        valueSets[valueAttributeKey].push(x);
                        return values.push(x);
                    });
//                    console.log(valueSets);

                    colorScaleSets = {};
                    Object.keys(valueSets).forEach(function(key){
                        if(valueSets.hasOwnProperty(key)){
                            colorScaleSets[key] = colorScaleGenerator(valueSets[key], key)
                        }
                    });

                    return forEachCell(function (x, elem, valueAttributeKey) {
                        var opts = aggregationMap[valueAttributeKey];
                        if(opts && opts.renderEnhancement ==='heatmap'){                          
                            var cs = colorScaleSets[valueAttributeKey](x);
                            elem.css("background-color", cs);                          
                            if(cs !== "rgb(255,255,255)"){
                                elem.css("color","#FFFFFF");
                            }
                            return elem;
                        } else {
                            return elem;
                        }

                    });
                };
            })(this);
            switch (scope) {
                case "heatmap":
                    heatmapper(".pvtVal");
                    break;
                case "rowheatmap":
                    for (i = l = 0, ref1 = numRows; 0 <= ref1 ? l < ref1 : l > ref1; i = 0 <= ref1 ? ++l : --l) {
                        heatmapper(".pvtVal.row" + i);
                    }
                    break;
                case "colheatmap":
                    for (j = n = 0, ref2 = numCols; 0 <= ref2 ? n < ref2 : n > ref2; j = 0 <= ref2 ? ++n : --n) {
                        heatmapper(".pvtVal.col" + j);
                    }
            }
            heatmapper(".pvtTotal.rowTotal");
            heatmapper(".pvtTotal.colTotal");
            return this;
        };
    });

    var pivotTableRenderer = function (pivotData, opts) {
        var aggregator, c, colAttrs, colKey, colKeys, defaults, getClickHandler, getMouseEnterHandler, getMouseLeaveHandler, getMouseMoveHandler, i, j, r, result,
            rowAttrs, rowKey, rowKeys, spanSize, tbody, td, th, thead, totalAggregator, tr, txt, val, x,
            valueAttrs;
        //this will be used to store the string representation of the table
        const stringRows = [];
        defaults = {
            table: {
                clickCallback: null,
                mouseEnterCallback: null,
                mouseLeaveCallback: null,
                mouseMoveCallback: null,
                rowTotals: true,
                colTotals: true
            },
            localeStrings: {
                totals: "Totals"
            },
            //group rows
            group: true,
            //show subtotals per row
            rowSubtotal: false,
            //show subtotals per column
            columnSubtotal: false
        };
        opts = $.extend(true, {}, defaults, opts);
        colAttrs = pivotData.colAttrs;
        rowAttrs = pivotData.rowAttrs;
        //now valueAttrs will come from analytics keys
        valueAttrs = [];//pivotData.valAttrs;
        rowKeys = pivotData.getRowKeys();
        colKeys = pivotData.getColKeys();
        //extra column/row span used to correct the size of cell in some particular scenarios
        //such as a table with no row/column dimensions
        var extraRowSpan = 1;
        var extraColSpan = 1;        

        var aggregationMap = {};
        if(opts.table.aggregationConfig){

            Object.keys(opts.table.aggregationConfig).forEach(function(key){
                if(opts.table.aggregationConfig.hasOwnProperty(key)){
                    var conf = opts.table.aggregationConfig[key];
                    aggregationMap[conf.name] = conf;
                }
            });

        }
        // get cell field type
        var fieldType = (label) => {
            if (label !== null && label !== "undefined" && label !== "") {
                let type = opts.fieldTypes.filter(x => x.label === label);
                if (type.length > 0) {
                    return type[0].fieldType;
                }
            }
            return 'String';
        }
        //First get to know what does the value list look like?
        var aggregator = pivotData.getAggregator([], []);

        var multipleValues = aggregator.multivalue();
        Object.keys(multipleValues).forEach(function(key){
            if(multipleValues.hasOwnProperty(key)){
                valueAttrs.push(key);
            }
        });
//        console.log(valueAttrs);

        var enableDebug = !!window.enablePivotDebug;



        if (opts.table.clickCallback) {
            getClickHandler = function (value, rowValues, colValues) {
                var attr, filters, i;
                filters = {};
                for (i in colAttrs) {
                    if (!hasProp.call(colAttrs, i)) continue;
                    attr = colAttrs[i];
                    if (colValues[i] != null) {
                        filters[attr] = colValues[i];
                    }
                }
                for (i in rowAttrs) {
                    if (!hasProp.call(rowAttrs, i)) continue;
                    attr = rowAttrs[i];
                    if (rowValues[i] != null) {
                        filters[attr] = rowValues[i];
                    }
                }
                return function (e) {
                    return opts.table.clickCallback(e, value, filters, pivotData);
                };
            };
        }

        if (opts.table.mouseEnterCallback) {
            getMouseEnterHandler = function (value, rowValues, colValues) {
                var attr, filters, i;
                filters = {};
                for (i in colAttrs) {
                    if (!hasProp.call(colAttrs, i)) continue;
                    attr = colAttrs[i];
                    if (colValues[i] != null) {
                        filters[attr] = colValues[i];
                    }
                }
                for (i in rowAttrs) {
                    if (!hasProp.call(rowAttrs, i)) continue;
                    attr = rowAttrs[i];
                    if (rowValues[i] != null) {
                        filters[attr] = rowValues[i];
                    }
                }
                return function (e) {
                    return opts.table.mouseEnterCallback(e, value, filters, pivotData);
                };
            };
        }

        if (opts.table.mouseLeaveCallback) {
            getMouseLeaveHandler= function (value, rowValues, colValues) {
                var attr, filters, i;
                filters = {};
                for (i in colAttrs) {
                    if (!hasProp.call(colAttrs, i)) continue;
                    attr = colAttrs[i];
                    if (colValues[i] != null) {
                        filters[attr] = colValues[i];
                    }
                }
                for (i in rowAttrs) {
                    if (!hasProp.call(rowAttrs, i)) continue;
                    attr = rowAttrs[i];
                    if (rowValues[i] != null) {
                        filters[attr] = rowValues[i];
                    }
                }
                return function (e) {
                    return opts.table.mouseLeaveCallback(e, value, filters, pivotData);
                };
            };
        }

        if (opts.table.mouseMoveCallback) {
            getMouseMoveHandler= function (value, rowValues, colValues) {
                var attr, filters, i;
                filters = {};
                for (i in colAttrs) {
                    if (!hasProp.call(colAttrs, i)) continue;
                    attr = colAttrs[i];
                    if (colValues[i] != null) {
                        filters[attr] = colValues[i];
                    }
                }
                for (i in rowAttrs) {
                    if (!hasProp.call(rowAttrs, i)) continue;
                    attr = rowAttrs[i];
                    if (rowValues[i] != null) {
                        filters[attr] = rowValues[i];
                    }
                }
                return function (e) {
                    return opts.table.mouseMoveCallback(e, value, filters, pivotData);
                };
            };
        }
        result = document.createElement("table");
        result.className = "pvtTable";

        /**
         * Function to get the span
         * @param arr
         * @param i
         * @param j
         * @returns {number}
         */
        spanSize = function (arr, i, j) {
            var l, len, n, noDraw, ref, ref1, stop, x;
            if (i !== 0) {
                noDraw = true;
                for (x = l = 0, ref = j; 0 <= ref ? l <= ref : l >= ref; x = 0 <= ref ? ++l : --l) {
                    if (arr[i - 1][x] !== arr[i][x]) {
                        noDraw = false;
                    }
                }
                if (noDraw) {
                    return -1;
                }
            }
            len = 0;
            while (i + len < arr.length) {
                stop = false;
                for (x = n = 0, ref1 = j; 0 <= ref1 ? n <= ref1 : n >= ref1; x = 0 <= ref1 ? ++n : --n) {
                    if (arr[i][x] !== arr[i + len][x]) {
                        stop = true;
                    }
                }
                if (stop) {
                    break;
                }
                len++;
            }
            return len;
        };

        thead = document.createElement("thead");

        function renderHeaderForTotal(label) {
            if (parseInt(j) === 0 && opts.table.rowTotals) {
                th = document.createElement("th");
                th.className = "pvtTotalLabel pvtRowTotalLabel";
                th.innerHTML = opts.localeStrings.totals;

                var numHiddenMeasures = 0;
                Object.keys(aggregationMap).forEach(function (key) {
                    if (aggregationMap[key].hideInTotals) {
                        numHiddenMeasures ++;
                    }
                });

                th.setAttribute("colspan", valueAttrs.length - numHiddenMeasures);
                //Added 1 for value headers              
                th.setAttribute("rowspan", colAttrs.length);
                tr.appendChild(th);
            }
        }        
        
        //creates a new columnkeyarray
        // the pivottables uses an column key array which is passed as parameter in the options
        // and defines the columns that are going to be shown in the pivottable
        // this pluging 'multifact-pivottable' creates as many 'total columns' (ex. Cost to client, clicks, etc) as
        // column keys we have. The idea here, if we have the column subtotal option on, is to add another column key ('Total')
        // adding that column key will force the code to add the total columns for it
        // the result would be something like this: 
        // +------------------------+------------------------+
        // | 2019                   | Totals                 |
        // +----------------+-------+----------------+-------+
        // | Cost to Client | Click | Cost to Client | Click |
        // +----------------+-------+----------------+-------+
        // | 0              | 0     | 0              | 0     |
        // +----------------+-------+----------------+-------+
        var auxiliarColumnKey = [];  
        if (opts.columnSubtotal && colKeys.length > 0 && colKeys[0].length > 1) {
            var positions = {};
            for (i in colKeys) {
                var ck = _.cloneDeep(colKeys[i]);
                auxiliarColumnKey.push(colKeys[i]);
                for (j in ck.reverse()) {
                    //get the amount of rows that the group will use.
                    x = spanSize(colKeys, parseInt(i),
                        parseInt(colKeys[i].length - parseInt(j) - 1));
                    //if the span size is more than one row and we are not in the last row key save it
                    if (x > 0 && j > 0 && !positions.hasOwnProperty(ck[j])) {
                        //saving header col position and rowspan
                        positions[ck[j]] = {pos: 0, q: 0};
                        positions[ck[j]].pos = parseInt(j);
                        positions[ck[j]].q = x;
                    }

                    if (positions.hasOwnProperty(ck[j]) &&
                        positions[ck[j]].pos === parseInt(j) &&
                        (positions[ck[j]].q - 1) === 0) {
                        //creates a new rowKey like [APAC, ASIA, subtotal]
                        delete positions[ck[j]];
                        var newColKey = colKeys[parseInt(i)].slice(0,
                            colKeys[parseInt(i)].length - j);
                        auxiliarColumnKey.push(newColKey.concat(
                            MMP.Localization.Reporting.ReportStatusView.PivotTable.colTotalsHeader));
                    }

                    if (positions.hasOwnProperty(ck[j]) &&
                        !((ck[j].q - 1) === 0)) {
                        positions[ck[j]].q--;
                    }
                }
            }          
        } else {
            auxiliarColumnKey = colKeys;
        }
        //creates each header in the pivottalbe for each column key
        function renderHeadersForValues(label) {     
            for (i in auxiliarColumnKey) {
                if (!hasProp.call(auxiliarColumnKey, i)) continue;
                colKey = auxiliarColumnKey[i];
                x = spanSize(auxiliarColumnKey, parseInt(i), parseInt(j));

                if (x !== -1) {
                    th = document.createElement("th");
                    th.className = "pvtColLabel";

                    //adjust colspan according to multiple variables
                    x = x * valueAttrs.length;


                    th.textContent = colKey[j];
                    th.setAttribute("colspan", x);
                    th.setAttribute("data-cell-type", fieldType(label));

                    if (parseInt(j) === colAttrs.length - 1 && rowAttrs.length !== 0) {
                        th.setAttribute("rowspan", extraRowSpan);
                    }
                    //if the column is the total one then add the appropriate class to it
                    if(colKey[j] === MMP.Localization.Reporting.ReportStatusView.PivotTable.colTotalsHeader){                        
                        th.classList.add('totals');
                    }
                    tr.appendChild(th);
                }
            }
        }

        for (j in colAttrs) {
            if (!hasProp.call(colAttrs, j)) continue;
            c = colAttrs[j];
            tr = document.createElement("tr");
            if (parseInt(j) === 0 && rowAttrs.length > 1) {
                th = document.createElement("th");
                th.setAttribute("colspan", rowAttrs.length - 1);
                th.setAttribute("rowspan", colAttrs.length);
                th.className = "pvtBlank";
                tr.appendChild(th);
            }
            th = document.createElement("th");
            th.className = "pvtAxisLabel";
            th.textContent = c;
            tr.appendChild(th);
            if(opts.table.prependRowTotals){
                renderHeaderForTotal(c);
                renderHeadersForValues(c);
            } else {
                renderHeadersForValues(c);
                renderHeaderForTotal(c);
            }

            thead.appendChild(tr);
        }
        
        var rowsHeaders = [];
        var totalsHeader = [];
        if (rowAttrs.length !== 0) {            
            for (i in rowAttrs) {
                if (!hasProp.call(rowAttrs, i)) continue;
                r = rowAttrs[i];
                th = document.createElement("th");
                th.className = "pvtAxisLabel";
                th.textContent = r;
                //storing row headers
                rowsHeaders.push($(th));               
            }
            th = document.createElement("th");
            if (colAttrs.length === 0) {
                th.className = "pvtTotalLabel pvtRowTotalLabel";
                th.setAttribute("colspan", valueAttrs.length);
                th.innerHTML = opts.localeStrings.totals;
                //storing column total headers
                totalsHeader.push($(th));
            }
            rowsHeaders[rowsHeaders.length - 1].attr('colspan', colKeys.length > 0 ? extraColSpan : 0);
        }
        result.appendChild(thead);
        tbody = document.createElement("tbody");       
        tbody.classList.add("clusterize-no-data");
        tbody.id = "contentAreaBody"
        /**
         *
         *
         *
         * Following Part Adds the headers for multiple measures
         *
         *
         *
         *
         */
        //Setting up the value headers

        tr = document.createElement("tr");
        //blank header
        th = document.createElement("th");
        th.textContent = '';
        //Add 1 to end if there are no cols
        th.setAttribute("colspan", rowAttrs.length+(colKeys.length > 0 ? extraRowSpan : 0));
        th.className = "pvtMeasureBlank";
        //adding blank header if there is no columns 
        if (colAttrs.length === 0) {
            var row = $('<tr>');
            totalsHeader.push(th);
            row.append(totalsHeader.reverse());
            tbody.appendChild(row[0]);
        }
        //adding a blank header if there is no rows
        if (rowAttrs.length === 0) {           
            rowsHeaders.push($(th));
        }
        $(tr).append(rowsHeaders);

        /**
         * Add the headers for multiple measures here
         */
        function renderMeasureHeadersForValues() {           
            //Add the headers for multiple measures here
            for (var k = 0, x = auxiliarColumnKey.length; k < x; k++) {
                var colKey = auxiliarColumnKey[k];
                if (!!colKey) {
                    var idx = 0;
                    for (var _key in valueAttrs) {
                        th = document.createElement("th");
                        th.className = "pvtMeasureLabel";
                        th.textContent = valueAttrs[idx++];
                        tr.appendChild(th);
                    }
                }
            }
        }

        /**
         *  For the Totals header to have the headers of multiple measures
         */
        function renderMeasuredHeadersForTotals() {
            if (opts.table.rowTotals) {
                for (var l = 0, x = valueAttrs.length; l < x; l++) {
                    var valAttr = valueAttrs[l];
                    if (aggregationMap[valAttr] && aggregationMap[valAttr].hideInTotals) {
                        continue;
                    }
                    th = document.createElement("th");
                    th.className = "pvtMeasureLabel pvtMeasureTotalLabel";
                    th.textContent = valAttr;
                    tr.appendChild(th);

                }
            }
        }

        if(opts.table.prependRowTotals){
            renderMeasuredHeadersForTotals();
            renderMeasureHeadersForValues();
        } else {
            renderMeasureHeadersForValues();
            renderMeasuredHeadersForTotals();
        }

        tbody.appendChild(tr);
        var subtotals = {};
        var colSubtotal = {};  
        // if the grouprow options is on then a new row is going to be added with the values we've already accumulated in renderValueCells function
        function renderSubtotalCells() {  
            //get the next accumulator
            subtotals = getNextAccumulator(accumulators);
            //we have subtotals per column and here we iterate each of them in order tu put the correct value on each subtotal cell
            for (var col in subtotals){
                for (var stat in subtotals[col]) {
                    td = document.createElement('td');
                    // td.className = 'pvtVal row' + i + ' col' + j + ' stat' + idx;
                    var valueSpan = $('<span>');
                    if(subtotals[col][stat].aggregator && subtotals[col][stat].acc) {
                        valueSpan.append(
                            $('<span>').html(subtotals[col][stat].aggregator.format(subtotals[col][stat].acc, stat)));
                    }
                    //if the accumulator doesn't have anything then show a 0
                    if(subtotals[col][stat].acc === 0){
                        valueSpan.append(
                            $('<span>').html("0"));
                    }
                 
                    td.append(valueSpan[0]);
                    td.setAttribute('data-value', val);
                    td.setAttribute('data-row', i);
                    td.setAttribute('data-stat-index', idx);
                    td.setAttribute('data-col', j);
                    td.setAttribute('data-value-for', stat);
                    td.setAttribute("data-cell-type", fieldType(stat));
                    td.className = 'subtotal';
                    tr.appendChild(td);
                    stringRows.push(tr.outerHTML);
                }
            }
            //the subtotals is set to empty when the first element of the rowkeys changes
            subtotals = {};
        }
        function renderValueCells() {
            for (j in auxiliarColumnKey) {
                if (!hasProp.call(auxiliarColumnKey, j)) continue;
                colKey = auxiliarColumnKey[j];
                aggregator = rowKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.rowTotalsHeader) ? pivotData.getAggregator([], colKey) :  pivotData.getAggregator(rowKey, colKey);
                val = aggregator.value();

                // Keep track on when we start the rendering
                opts.renderStart = opts.renderStart || new Date();
                //  Check how long the report has been rendering and stop if it has been processing for too long
                var elapsedTime = new Date() - opts.renderStart;
                if (elapsedTime > opts.longRunningReportTimeoutSeconds * 1000) {
                    // We have been executing for over a minute, use the callback and stop the process
                    if (opts.longRunningReportCallback) {
                        opts.longRunningReportCallback(elapsedTime / 1000);
                    }
                    throw "The report has been rendering for too long. Stopping the process.";
                }

                /**
                 * In this section we are adding values to these cells
                 */
                

                if (!!aggregator.multivalue) {                   
                    var stats = aggregator.multivalue(rowKey, colKey);
                    var idx = 0;
                    for (var stat in stats) {
                        val = stats[stat];
                        //if the row subtotal is on then start accumulating each value for each row and column                       
                        if (opts.rowSubtotal) {   
                            createRowSubtotalProperties(j,stat, accumulators);                         
                            createColumnSubtotalProperties(j,stat, accumulators);                          
                            //accumulates the values
                            if(stats[stat] && stats[stat] !== "") {                              
                                setValuesToAccumulators(j,stat, stats[stat], aggregator, accumulators);
                            }
                        }                        
                        // if the iteration does not reach out to Totals, which is the new item we added to the columnkey array
                        // then continue accumulating the column values
                        if(opts.columnSubtotal && !colKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.colTotalsHeader)){
                            createRowSubtotalProperties(stat,"", columnAcc, true);                            
                            //if the stat already exists
                            if(stats[stat] && stat[stat] !== ""){                              
                                setValuesToAccumulators(stat, "", stats[stat], aggregator, columnAcc, true);
                            }
                        }
                        
                        td = document.createElement("td");
                        td.className = "pvtVal row" + i + " col" + j + " stat" + idx;
                        td.setAttribute("data-cell-type", fieldType(stat));
                        
                        //td.textContent = stat + ' : ' + aggregator.format(val);
                        //td.textContent = aggregator.format(val);
                        var valueSpan = $('<span>');
                        if (enableDebug) {
                            valueSpan.append($('<span>').html(stat).addClass('small text-grey'));
                            valueSpan.append($('<br>'));
                        }
                        //when the iteration reach to the total column key added then put the values in the new cells
                        if (opts.columnSubtotal && colKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.colTotalsHeader)) {
                            var accum = getNextAccumulator(columnAcc);
                            if (acum && acum[stat]) {
                                val = accum[stat].acc;
                                aggregator = accum[stat].aggregator ||
                                    aggregator;                              
                            }
                            td.classList.add('totals');
                        }
                        valueSpan.append($('<span>').html(aggregator.format(val, stat)));

                        td.append(valueSpan[0]);
                        td.setAttribute("data-value", val);
                        td.setAttribute("data-row", i);
                        td.setAttribute("data-stat-index", idx);
                        td.setAttribute("data-col", j);
                        td.setAttribute("data-value-for", stat);
                        td.setAttribute("data-cell-type", fieldType(stat));
                        if (getClickHandler != null) {
                            td.onclick = getClickHandler(val, rowKey, colKey);
                        }

                        if (getMouseEnterHandler != null) {
                            td.onmouseenter = getMouseEnterHandler(val, rowKey, colKey);
                        }

                        if (getMouseLeaveHandler != null) {
                            td.onmouseleave = getMouseLeaveHandler(val, rowKey, colKey);

                        }

                        if (getMouseMoveHandler != null) {
                            td.onmouseleave = getMouseMoveHandler(val, rowKey, colKey);

                        }
                        tr.appendChild(td);
                        stringRows.push(tr.outerHTML);
                        idx++;
                    }


                } else {    
                            
                    for (var k = 0, x = valueAttrs.length; k < x; k++) {
                        var valueAttr = valueAttrs[k];

                        val = aggregator.value();
                        // if the iteration does not reach out to Totals, which is the new item we added to the columnkey array
                        // then continue accumulating the column values
                        if(opts.columnSubtotal && !colKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.colTotalsHeader)){
                            createRowSubtotalProperties(valueAttr,"", columnAcc, true);
                            
                            //if the stat already exists
                            if(val && val !== ""){                              
                                setValuesToAccumulators(valueAttr, "", val, aggregator, columnAcc, true);
                            }
                        }
                        
                        td = document.createElement("td");
                        td.className = "pvtVal row" + i + " col" + j;
                        td.textContent = aggregator.format(val) || "0";
                        //when the iteration reach to the total column key added then put the values in the new cells
                        if (opts.columnSubtotal && colKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.colTotalsHeader)) {                            
                            var acum = getNextAccumulator(columnAcc);
                            if (acum && acum[valueAttr]) {
                                val = _.cloneDeep(acum[valueAttr].acc);
                                aggregator = _.cloneDeep(acum[valueAttr].aggregator) || aggregator;                               
                            }
                            td.classList.add('totals');
                            td.textContent = aggregator.format(val, valueAttr) || "0";
                        }
                        
                        td.setAttribute("data-cell-type", fieldType(valueAttr));
                        
                       
                        td.setAttribute("data-value", val);
                        
                        if (getClickHandler != null) {
                            td.onclick = getClickHandler(val, rowKey, colKey);
                        }
                        tr.appendChild(td);
                        stringRows.push(tr.outerHTML);
                        //if the row subtotal is on then start accumulating each value for each row and column                     
                        if (opts.rowSubtotal) {
                            createRowSubtotalProperties(j,valueAttr, accumulators);                           
                            createColumnSubtotalProperties(j,valueAttr, accumulators);                          
                            if(val && val !== "") {                               
                                setValuesToAccumulators(j,valueAttr, val, aggregator, accumulators);
                            }
                        }
                    }
                }
            }
            return {stats, idx, valueSpan, k, x};
        }

        function renderRowTotalCells() {
            if (opts.table.rowTotals || colAttrs.length === 0) {
                totalAggregator = pivotData.getAggregator(rowKey, []);
                val = totalAggregator.value();

                //console.log(totalAggregator.multivalue())

                if (!!totalAggregator.multivalue) {
                    var stats = totalAggregator.multivalue();
                    for (var stat in stats) {
                        val = stats[stat];
                        //here we had to accumulate values for each total column,
                        // these column are the one that already exist in the pivot table
                        // and are rendered at the end of each row
                        // but we need also to add the subtotals for them
                        if (opts.rowSubtotal) {
                                createRowSubtotalProperties(j + 1,stat, accumulators);
                                createColumnSubtotalProperties(j + 1,stat, accumulators);
                        
                            if(stats[stat] && stats[stat] !== "") {                               
                                setValuesToAccumulators(j + 1, stat, stats[stat], totalAggregator, accumulators);
                            }
                        }

                        if(aggregationMap[stat] && aggregationMap[stat].hideInTotals){
                            continue;
                        }

                        td = document.createElement("td");
                        td.className = "pvtTotal rowTotal";
                        //td.textContent = totalAggregator.format(val, k);

                        var valueSpan = $('<span>');
                        if (enableDebug) {
                            valueSpan.append($('<span>').html(stat).addClass('small text-grey'));
                            valueSpan.append($('<br>'));
                        }
                        valueSpan.append($('<span>').html(totalAggregator.format(val, stat)));

                        td.append(valueSpan[0]);

                        td.setAttribute("data-value", val);
                        td.setAttribute("data-value-for", stat);
                        td.setAttribute("data-cell-type", fieldType(stat));
                        if (getClickHandler != null) {
                            td.onclick = getClickHandler(val, rowKey, []);
                        }

                        td.setAttribute("data-for", "row" + i);
                        tr.appendChild(td);
                        stringRows.push(tr.outerHTML);
                    }

                } else {

                    for (var k = 0, x = valueAttrs.length; k < x; k++) {
                        var valueAttr = valueAttrs[k];

                        val = totalAggregator.value();
                        td = document.createElement("td");
                        td.className = "pvtTotal rowTotal";
                        td.textContent = totalAggregator.format(val);
                        td.setAttribute("data-value", val);
                        if (getClickHandler != null) {
                            td.onclick = getClickHandler(val, rowKey, []);
                        }
                        td.setAttribute("data-for", "row" + i);
                        tr.appendChild(td);
                        stringRows.push(tr.outerHTML);
                        //here we had to accumulate values for each total column,
                        // these column are the one that already exist in the pivot table
                        // and are rendered at the end of each row
                        // but we need also to add the subtotals for them
                        if (opts.rowSubtotal) {
                            if (!subtotals[j]) {
                                subtotals[j] = {[valueAttr]:{acc: 0, aggregator: undefined}};                                
                            }

                            createRowSubtotalProperties(j,valueAttr, accumulators);

                            if (!subtotals[j][valueAttr]) {
                                subtotals[j][valueAttr] = {acc: 0, aggregator: undefined};                                
                            }
                            createColumnSubtotalProperties(j,valueAttr, accumulators);
                            
                            if(val && val !== "") {
                                subtotals[j][valueAttr].acc =  subtotals[j][valueAttr].acc + val;
                                subtotals[j][valueAttr].aggregator = totalAggregator;
                                setValuesToAccumulators(j, valueAttr, val, totalAggregator, accumulators);
                            }
                        }
                    }
                }
            }
            return {stats, valueSpan, k, x};
        }

        var auxililarRowKey = [];  
        
        //the idea here is to create one accumulator per group
        /**
         * Adds accumulators to the two existing accumulators (rows or columns)
         * The idea is to accumulate each value and show it for a row or for a column
         * @param keys - columns or rows names
         * @param accumulators - one of the two accumulators created
         */
	    var createAccumulators = function(keys, accumulators) {
	        if(keys.length > 0) {	          
                keys[0].forEach(function(rk){
                    accumulators.push({});
                });
            }        
	    };

        /**
         * Creates an accumulator per fact / column and column for the row subtotals or
         * creates an accumulator per fact for the column subtotals
         * @param column - name of the fact / column
         * @param stat - row number
         * @param acc - accumulator array to add the new accumulators
         * @param excludeStat - for the column subtotals we don't need to add the row so set this as true
         */
	    var createRowSubtotalProperties = function(column, stat, acc, excludeStat){	        
		    //iterate through the acccumulators and create a new subtotal row
            acc.forEach(function(accumulator){
		        if(!accumulator[column]){
		            if(excludeStat){
                        accumulator[column] = {acc: 0, aggregator: undefined};
		            } else {
                        accumulator[column] = {[stat]:{acc: 0, aggregator: undefined}};
                    }
                }
		    })
	    }

        /**
         * Creates a new empty column Subtotal
         * @param column - column/ fact name
         * @param stat - row number
         * @param acc - accumulator array
         */
	    var createColumnSubtotalProperties = function(column, stat, acc) {
	    	//iterate through the accumulators and create new subtotal columns
            acc.forEach(function(accumulator){
		        if(!accumulator[column][stat]){
                    accumulator[column][stat] = {acc: 0, aggregator: undefined};
                }
		    })
	    }

        /**
         * Gets the next accumulator based on the acc array 
         * @param acc - accumulator array
         * @return {*} - accumulator
         */
	    var getNextAccumulator = function(acc){
	        for(i = 0; i < acc.length; i++){
	            if(!jQuery.isEmptyObject(acc[i])){
	                var item = _.cloneDeep(acc[i]);
	                //empty the accumulator
	                acc[i] = {};
	                return item;  
                }
            }
        }

        /**
         * Sets values to each accumulator
         * @param column - column/fact name
         * @param stat - row
         * @param newVal - new value for the accumulators
         * @param aggregator - aggregator used
         * @param acc - accumulator array
         * @param excludeStat -  for the column subtotals we don't need to search for the row
         */
	    var setValuesToAccumulators = function(column, stat, newVal, aggregator, acc, excludeStat){
	    	//iterate through the accumulators and accumulate the new value            
            acc.forEach(function(accumulator) {
                if (excludeStat) {
                    accumulator[column].acc = accumulator[column].acc + newVal;
                    accumulator[column].aggregator = aggregator;
                } else {
                    accumulator[column][stat].acc = accumulator[column][stat].acc + newVal;
                    accumulator[column][stat].aggregator = aggregator;
                }
		    })
	    }

        /**
         * For each row in data-table
         */
        // the pivottables uses a row key array which is passed as parameter in the options
        // and defines the rows that are going to be shown in the pivottable
        // The idea here, if we have the row subtotal option on, is to add another row key ('subtotal')
        // adding that row key will force the code to add a new subbtotal row for each row group
        // the result would be something like this: 
        // +------------+-------------------------------------+
        // | year       | 2019                                |
        // +------------+------------+----------------+-------+
        // |            |            | Cost to Client | Click |
        // +------------+------------+----------------+-------+
        // | media type | cinema     | 1              | 0     |
        // |            +------------+----------------+-------+
        // |            | television | 1              | 1     |
        // |            +------------+----------------+-------+
        // |            | subtotal   | 2              | 1     |
        // +------------+------------+----------------+-------+
        if(opts.rowSubtotal && rowKeys.length > 0 && rowKeys[0].length > 1) {
            var positions = {};
            for (i in rowKeys){             
                var rk = _.cloneDeep(rowKeys[i]);
                auxililarRowKey.push(rowKeys[i]);
                for (j in rk.reverse()){
                    //get the amount of rows that the group will use.
                    x = spanSize(rowKeys, parseInt(i), parseInt( rowKeys[i].length - parseInt(j) - 1));
                    //if the span size is more than one row and we are not in the last row key save it
                    if ( x > 0 && j > 0 && !positions.hasOwnProperty(rk[j])) {
                        //saving header col position and rowspan
                        positions[rk[j]] = {pos: 0, q: 0};
                        positions[rk[j]].pos = parseInt(j);
                        positions[rk[j]].q = x;
                    }

                    if(positions.hasOwnProperty(rk[j]) && positions[rk[j]].pos === parseInt(j) && (positions[rk[j]].q - 1) === 0) {
                        //creates a new rowKey like [APAC, ASIA, subtotal]
                        delete positions[rk[j]];
                        var newRowKey = rowKeys[parseInt(i)].slice(0, rowKeys[parseInt(i)].length - j);
                        auxililarRowKey.push(newRowKey.concat(MMP.Localization.Reporting.ReportStatusView.PivotTable.rowTotalsHeader));
                    }

                    if(positions.hasOwnProperty(rk[j]) && !((rk[j].q - 1) === 0)){
                        positions[rk[j]].q --;
                    }

                }
            }           
        } else {
            auxililarRowKey = rowKeys;
        }
        
        //create row accumulators
        var accumulators = [];
        if(opts.rowSubtotal) {
            createAccumulators(rowKeys, accumulators);
        }
        
        //create column accumulators
        var columnAcc = [];
        if (opts.columnSubtotal) {
            createAccumulators(colKeys, columnAcc);
        }
        
        for (i in auxililarRowKey) {          
            //Omit the proto props
            // if (!hasProp.call(rowKeys, i)) continue;
            if (!hasProp.call(auxililarRowKey, i)) continue;

            // rowKey = rowKeys[i];
            rowKey = auxililarRowKey[i];

            /**
             * Create a tr (row) element for each rowKey
             * @type {HTMLTableRowElement}
             */
            tr = document.createElement("tr");
            for (j in rowKey) {
                if (!hasProp.call(rowKey, j)) continue;
                txt = rowKey[j];

                //Get the rowspan for this label
                // x = spanSize(rowKeys, parseInt(i), parseInt(j));
                x = spanSize(auxililarRowKey, parseInt(i), parseInt(j));

                if (x !== -1 || !opts.group) {
                    th = document.createElement("th");
                    th.className = "pvtRowLabel";
                    th.textContent = txt;
                    th.setAttribute("data-cell-type", fieldType(rowAttrs[parseInt(j)]));
                    if (opts.group) {
                        th.setAttribute('rowspan', x);
                    }                   
                    if (parseInt(j) === rowAttrs.length - 1 && colAttrs.length !== 0) {
                        th.setAttribute("colspan", extraColSpan);
                    }
                    //adds a subtotal row header
                    if(txt === MMP.Localization.Reporting.ReportStatusView.PivotTable.rowTotalsHeader && opts.rowSubtotal) {
                        th.setAttribute("colspan", rowAttrs.length + extraColSpan - rowKey.length);
                        th.classList.add('subtotal');
                    }
                    tr.appendChild(th);
                    stringRows.push(tr.outerHTML);
                }
            }

            if(opts.table.prependRowTotals){
                //add the subtotal rows if the option rowSubtotal is 'on'               
                if (rowKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.rowTotalsHeader) && opts.rowSubtotal) {
                    renderSubtotalCells();
                } else {
                    var {stats, valueSpan, k, x} = renderRowTotalCells();
                    var {stats, idx, valueSpan, k, x} = renderValueCells();                  
                }
            } else {
                //add the subtotal rows if the option rowSubtotal is 'on'
                if (rowKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.rowTotalsHeader) && opts.rowSubtotal) {
                    renderSubtotalCells();
                } else {
                    var {stats, idx, valueSpan, k, x} = renderValueCells();
                    var {stats, valueSpan, k, x} = renderRowTotalCells();
                }               
            }

            tbody.appendChild(tr);
        }

        function renderColumnTotals() {
            //iterates through  each column key and render the values in the pivottable          
            for (j in auxiliarColumnKey) {               
                if (!hasProp.call(auxiliarColumnKey, j)) continue;
                colKey = auxiliarColumnKey[j];
                totalAggregator = pivotData.getAggregator([], colKey);
                val = totalAggregator.value();


                if (!!totalAggregator.multivalue) {
                    var stats = totalAggregator.multivalue();
                    for (var stat in stats) {
                        val = stats[stat];
                        if(opts.columnSubtotal && !colKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.colTotalsHeader)){
                            createRowSubtotalProperties(stat,"", columnAcc, true);
                            //if the stat already exists
                            if(stats[stat] && stat[stat] !== ""){                              
                                setValuesToAccumulators(stat, "", stats[stat], totalAggregator, columnAcc, true);
                            }
                        }
                        //when the iteration reach to the total column key added then put the values in the new cells
                        if (opts.columnSubtotal && colKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.colTotalsHeader)) {
                            var accum = getNextAccumulator(columnAcc);
                            if (acum && acum[stat]) {
                                val = accum[stat].acc;
                                colSubtotal[stat].acc = 0;
                                colSubtotal[stat].aggregator = undefined;                                
                            }
                            td.classList.add('totals');
                        }
                        
                        td = document.createElement("td");
                        td.className = "pvtTotal colTotal";
                        //td.textContent = totalAggregator.format(val);

                        var valueSpan = $('<span>');
                        if (enableDebug) {
                            valueSpan.append($('<span>').html(stat).addClass('small text-grey'));
                            valueSpan.append($('<br>'));
                        }
                        valueSpan.append($('<span>').html(totalAggregator.format(val, stat) || "0"));

                        td.append(valueSpan[0]);

                        td.setAttribute("data-value", val);
                        td.setAttribute("data-value-for", stat);
                        td.setAttribute("data-cell-type", fieldType(stat));
                        if (getClickHandler != null) {
                            td.onclick = getClickHandler(val, [], colKey);
                        }
                        td.setAttribute("data-for", "col" + j);
                        tr.appendChild(td);
                        stringRows.push(tr.outerHTML);
                    }

                } else {

                    for (var k = 0, x = valueAttrs.length; k < x; k++) {
                        var valueAttr = valueAttrs[k];
                        if(opts.columnSubtotal && !colKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.colTotalsHeader)){
                           createRowSubtotalProperties(valueAttr,"", columnAcc, true);                           
                            if(val && val !== ""){                             
                                setValuesToAccumulators(valueAttr, "", val, totalAggregator, columnAcc, true);
                            }
                        }
                        val = totalAggregator.value();
                        td = document.createElement("td");
                        td.className = "pvtTotal colTotal";
                        td.textContent = totalAggregator.format(val);
                        if (opts.columnSubtotal && colKey.includes(MMP.Localization.Reporting.ReportStatusView.PivotTable.colTotalsHeader)) {                                                      
                            var acum = getNextAccumulator(columnAcc);
                            if (acum && acum[valueAttr]) {
                                totalAggregator = _.cloneDeep(
                                    acum[valueAttr].aggregator) ||
                                    totalAggregator;
                                val = _.cloneDeep(acum[valueAttr].acc);                                
                            }
                            td.classList.add('totals');
                            td.textContent = totalAggregator.format(val,
                                valueAttr) || "0";
                        }
                        td.setAttribute("data-value", val);
                        if (getClickHandler != null) {
                            td.onclick = getClickHandler(val, [], colKey);
                        }
                        td.setAttribute("data-for", "col" + j);
                        tr.appendChild(td);
                        stringRows.push(tr.outerHTML);
                    }
                }


            }
        }

        function renderGrandTotals() {
            if (opts.table.rowTotals || auxiliarColumnKey.length === 0) {
                totalAggregator = pivotData.getAggregator([], []);
                val = totalAggregator.value();


                if (!!totalAggregator.multivalue) {
                    var stats = totalAggregator.multivalue();
                    for (var stat in stats) {

                        if(aggregationMap[stat] && aggregationMap[stat].hideInTotals){
                            continue;
                        }

                        val = stats[stat];

                        td = document.createElement("td");
                        td.className = "pvtGrandTotal";
                        td.textContent = totalAggregator.format(val, stat);
                        td.setAttribute("data-value", val);
                        td.setAttribute("data-value-for", stat);
                        td.setAttribute("data-cell-type", fieldType(stat));
                        if (getClickHandler != null) {
                            td.onclick = getClickHandler(val, [], []);
                        }
                        tr.appendChild(td);
                        stringRows.push(tr.outerHTML);
                    }

                } else {

                    for (var k = 0, x = valueAttrs.length; k < x; k++) {
                        var valueAttr = valueAttrs[k];

                        val = totalAggregator.value();
                        td = document.createElement("td");
                        td.className = "pvtGrandTotal";
                        td.textContent = totalAggregator.format(val);
                        td.setAttribute("data-value", val);
                        if (getClickHandler != null) {
                            td.onclick = getClickHandler(val, [], []);
                        }
                        tr.appendChild(td);
                        stringRows.push(tr.outerHTML);
                    }
                }
            }
        }

        if (opts.table.colTotals || rowAttrs.length === 0) {
            tr = document.createElement("tr");
            if (opts.table.colTotals || rowAttrs.length === 0) {
                th = document.createElement("th");
                th.className = "pvtTotalLabel pvtColTotalLabel";
                th.innerHTML = opts.localeStrings.totals;
                th.setAttribute("colspan", rowAttrs.length);
                tr.appendChild(th);
                stringRows.push(tr.outerHTML);
            }

            if(opts.table.prependRowTotals){
                renderGrandTotals();
                renderColumnTotals();
            } else {
                renderColumnTotals();
                renderGrandTotals();
            }


            tbody.appendChild(tr);
        }
        result.appendChild(tbody);
        result.setAttribute("data-numrows", rowKeys.length);
        result.setAttribute("data-numcols", colKeys.length);

        // Check the number of cells that are included in the report to avoid rendering if too big
        var cellCount = result.getElementsByTagName("td").length;
        if (cellCount > opts.longRunningReportMaxCells) {
            opts.longRunningReportCallback(null, cellCount);
            throw "The report is too big. Stopping the process.";
        }

        return {htmlData: result, stringResult: stringRows};
    };

    $.pivotUtilities.gtRenderers = {
        "Table": function (pivotData, opts) {
            return pivotTableRenderer(pivotData, opts);
        },
        "Table Heatmap": function (pivotData, opts) {
            return $(pivotTableRenderer(pivotData, opts)).gtHeatmap("heatmap", opts);
        },
        "Table Heatmap and Barchart": function (pivotData, opts) {
            return $($(pivotTableRenderer(pivotData, opts)).gtHeatmap("heatmap", opts)).gtBarchart(opts);
        },
        "Table Row Heatmap": function (pivotData, opts) {
            return $(pivotTableRenderer(pivotData, opts)).gtHeatmap("rowheatmap", opts);
        },
        "Table Col Heatmap": function (pivotData, opts) {
            return $(pivotTableRenderer(pivotData, opts)).gtHeatmap("colheatmap", opts);
        }
    };
    

}).call(this);

