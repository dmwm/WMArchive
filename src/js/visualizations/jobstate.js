var app = app || {};
app.visualizationViews = app.visualizationViews || {};

app.visualizationViews['jobstate'] = Backbone.View.extend({

  title: 'Jobstate',

  initialize: function(options) {
    _.extend(this, _.pick(options, 'data', 'axis'));
  },

  render: function() {
    this.$el.empty();

    var self = this;
    var data = this.data;

    if (data.length <= 5) {

      // Preprocess data
      var all_jobstates = new Set();
      for (var d of data) {
        for (var dd of d.jobstates) {
          all_jobstates.add(dd.jobstate);
        }
      }
      all_jobstates = Array.from(all_jobstates);
      var max_count = d3.max(data, function(d) { return d3.sum(d.jobstates, function(dd) { return dd.count; }) });

      // Setup scales
      var length = d3.scaleLinear()
        .range([ 0, 100 ])
        .domain([ 0, max_count ]);
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

      var canvas = d3.select(this.el);

      var stack = d3.stack()
          .keys(all_jobstates)
          .order(d3.stackOrderNone)
          .offset(d3.stackOffsetNone);
      var stack_data = stack(data.map(function(d) {
        var r = {
          label: d.label,
        };
        for (var jobstate of all_jobstates) {
          r[jobstate] = 0;
        }
        for (var dd of d.jobstates) {
          r[dd.jobstate] = dd.count;
        }
        return r;
      }));
      var container = canvas.selectAll('.full-width-container')
        .data(stack_data[0].map(function(col, i) {
          var new_row = stack_data.map(function(row) {
            var new_col = row[i];
            new_col.jobstate = row.key;
            return new_col;
          });
          new_row.label = col.data.label;
          return new_row;
        }))
        .enter()
          .append('div').attr('class', 'full-width-container');

      container.append('a')
        .attr('class', 'chart-label')
        .text(function(d) {
          return d.label;
        });

      var bar = container.append('svg')
        .attr('class', 'chart')

      bar.selectAll('rect')
        .data(function(d) {
          return d;
        })
        .enter().append('rect')
          .attr('height', '100%')
          .attr('x', function(d) {
            return length(d[0]) + '%';
          })
          .attr('y', 0)
          .attr('width', function(d) {
            return (length(d[1]) - length(d[0])) + '%';
          })
          .attr('fill', function(d) {
            return color(d.jobstate);
          })
          .attr('data-toggle', 'tooltip')
          .attr('title', function(d) {
            return d.jobstate + ': ' + (d[1] - d[0]) + ' jobs (' + numeral((d[1] - d[0]) / d3.sum(all_jobstates.map(function(jobstate) {
              return d.data[jobstate];
            }))).format('0.0%') + ')';
          })

    } else {

      var labels = data.map(function(d) { return d['label'] })
      var counts = data.map(function(d) { return d3.sum(d['jobstates'].map(function(dd) { return dd['count'] })) });
      var maxCount = d3.max(counts);

      var canvas = d3.select(this.el)
        .attr('class', 'canvas-flex');

      var container = canvas.selectAll('.site-count-container')
        .data(data)
        .enter()
          .append('div').attr('class', 'site-count-container')
      var pie = container.append('svg')
        .attr('class', 'chart')
        .attr('width', function(d) {
          return Math.sqrt(d3.sum(d['jobstates'].map(function(dd) { return dd['count'] })) / maxCount) * 200 + 'px';
        })
        .attr('viewBox', '0 0 100 100')
        .append("g")
          .attr("transform", 'translate(50,50)')
      var pieLayout = d3.pie()
        .value(function(d) {
          return d.count;
        })
        .sort(null);
      var path = pie.selectAll('path')
        .data(function(d) {
          return pieLayout(d.jobstates);
        })
        .enter()
          .append('path')
          .attr('d', d3.arc()
            .innerRadius(50 / 4)
            .outerRadius(50))
          .attr('fill', function(d, i) {
            switch (d.data.jobstate) {
              case 'success':
                return '#31AD64';
              case 'jobfailed':
                return '#E54E42';
              case 'submitfailed':
                return '#3081B8';
              default:
                return 'black';
            }
          });

      var label = container.append('a')
        .attr('class', 'chart-label')
        .text(function(d) {
          return d['label'];
        })

    }

    return this;
  },

  events: {
    'click a': 'refineFilter',
  },

  refineFilter: function(event) {
    app.scope.set(this.axis, event.target.text);
  },

});
