app = app || {};
app.visualizationRenderers = app.visualizationRenderers || {};

app.visualizationRenderers.heatmap = function(canvas, data, metric, axis, supplementaryData) {

  // Preprocess data
  var is_stacked = (data[0] || {})['jobstates'] != null;

  if (is_stacked) {
    var all_jobstates = new Set();
    for (var d of data) {
      for (var dd of d.jobstates) {
        all_jobstates.add(dd.jobstate);
      }
    }
    all_jobstates = Array.from(all_jobstates);

    var color = d3.scaleOrdinal()
      .range(all_jobstates.map(function(d) {
        switch (d) {
          case 'success':
            return '#31AD64';
          case 'jobfailed':
            return '#E54E42';
          case 'submitfailed':
            return '#3081B8';
          default:
            return 'black';
        }
      }))
      .domain(all_jobstates);
  }

  var axis_counts = data.map(function(d) {
    if (is_stacked) {
      return d3.sum(d.jobstates, function(dd) { return dd.count; })
    } else {
      return d.average;
    }
  });
  var max_count = d3.max(axis_counts);
  var total_count = d3.sum(axis_counts);


  var item_size = 50 * 10 / Math.sqrt(data.length);

  // Setup scales
  var strength = d3.scaleLinear()
    .range([ 0, 1 ])
    .domain([ 0, max_count ]);

  var bar_data = data;

  if (is_stacked) {
    var stack = d3.stack()
        .keys(all_jobstates)
        .order(d3.stackOrderNone)
        .offset(d3.stackOffsetExpand);
    var stack_data = stack(data.map(function(d) {
      var r = {
        label: d.label,
        totalCount: d3.sum(d.jobstates.map(function(j) { return j.count; }))
      };
      for (var jobstate of all_jobstates) {
        r[jobstate] = 0;
      }
      for (var dd of d.jobstates) {
        r[dd.jobstate] = dd.count;
      }
      return r;
    }));
    stack_data = stack_data[0].map(function(col, i) {
      var new_row = stack_data.map(function(row) {
        var new_col = row[i];
        new_col.jobstate = row.key;
        return new_col;
      });
      new_row.label = col.data.label;
      new_row.average = col.data.totalCount;
      return new_row;
    });
    bar_data = stack_data;
  }

  var container = canvas.append('div').attr('class', 'heatmap')

  var item = container.selectAll('.heatmap-item')
    .data(bar_data)
    .enter()
      .append('svg').attr('class', 'heatmap-item')
      .attr('width', item_size)
      .attr('height', item_size)
  var item_content = item.append('g')
    .attr('data-toggle', 'tooltip')
    .attr('title', function(d) {
      var label = d.label;
      if (self.axis == 'exitCode') {
        var desciption = (supplementaryData['exitCodes'] || {})[d['label']];
        if (desciption != null) {
          label += " - " + desciption;
        }
      }
      return label + ": " + app.format_value(metric)(d.average);
    })
    .attr('opacity', function(d) {
      return strength(d.average);
    });


  if (is_stacked) {
    var y = d3.scaleLinear()
      .range([ 0, 100 ])
    item_content.selectAll('rect')
      .data(function(d) {
        return d;
      })
      .enter().append('rect')
        .attr('width', '100%')
        .attr('y', function(d) {
          return y(d[0]) + '%';
        })
        .attr('x', 0)
        .attr('height', function(d) {
          return (y(d[1]) - y(d[0])) + '%';
        })
        .attr('fill', function(d) {
          return color(d.jobstate);
        })
    } else {
      item_content.append("rect")
        .attr('height', '100%')
        .attr('width', '100%')
        .attr('class', 'filled')
    }

    item_content.append("rect")
      .attr('height', '100%')
      .attr('width', '100%')
      .attr('data-scope-filter', function(d) {
        return d.label;
      })
      .attr('opacity', 0)

}
