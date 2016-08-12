var app = app || {};

app.FilterView = Backbone.View.extend({

  tagName: 'fieldset',
  className: 'filter-container form-group',

  template: _.template('<div class="dropdown form-group"><label class="form-control-label" for="<%=input_id%>"><%=label%></label><div class="filter-text-container"><input type="search" class="form-control filter-text" id="<%=input_id%>" placeholder="<%=placeholder%>"><span class="clear-filter">✕</span></div></div>'),

  initialize: function(params) {
    this.label = params.label;
    this.input_id = params.input_id;
    this.model = app.scope;
    this.model.on('change:' + this.input_id, this.render, this);
  },

  render: function() {
    var scope_key = this.input_id;
    var scope_value = this.model.get(scope_key);

    this.$el.html(this.template({ label: this.label.toUpperCase(), input_id: this.input_id, placeholder: "Filter by " + this.label + "…" }));

    var all_selection_options = new Bloodhound({
      datumTokenizer: Bloodhound.tokenizers.whitespace,
      queryTokenizer: Bloodhound.tokenizers.whitespace,
      local: (this.model.get('suggestions') || {})[scope_key] || [],
    });
    function selection_options(q, sync) {
      if (q === '') {
        sync(all_selection_options.all());
      } else {
        all_selection_options.search(q, sync);
      }
    }
    this.$('.filter-text').typeahead({
      hint: true,
      highlight: true,
      minLength: 0,
      classNames: {
        menu: 'dropdown-menu',
        suggestion: 'dropdown-item',
        cursor: 'dropdown-cursor',
      }
    },
    {
      name: this.input_id + '-options',
      source: selection_options,
      limit: 200,
    });

    this.$('.filter-text').val(scope_value);
    if (scope_value != null && scope_value != '') {
      this.$el.addClass('filter-active');
      this.$('.clear-filter').show();
    } else {
      this.$el.removeClass('filter-active');
      this.$('.clear-filter').hide();
    }

    this.delegateEvents(); // required to trigger events when added to parent view
    return this;
  },

  events: {
    'typeahead:select .filter-text': 'applyFilter',
    'click .clear-filter': 'clearFilter',
  },

  applyFilter: function(event, selection) {
    var scope_key = this.input_id;
    var scope_value = selection;

    if (scope_value == '') {
      scope_value = null;
    }

    this.model.set(scope_key, scope_value);
    this.model.fetch();
  },

  clearFilter: function(event, selection) {
    var scope_key = this.input_id;
    this.model.set(scope_key, null);
    this.model.fetch();
  },

});
