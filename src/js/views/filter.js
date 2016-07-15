var app = app || {};

app.FilterView = Backbone.View.extend({

  tagName: 'fieldset',
  className: 'filter-container form-group',

  template: _.template('<label for="<%=input_id%>"><%=label%></label><input type="text" class="form-control" id="<%=input_id%>" placeholder="<%=placeholder%>">'),

  initialize: function(params) {
    this.label = params.label;
    this.input_id = params.input_id;
  },

  render: function() {
    this.$el.html(this.template({ label: this.label.toUpperCase(), input_id: this.input_id, placeholder: "Filter by " + this.label + "…" }));
  },

});
