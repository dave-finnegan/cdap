/*
 * Text Field Embeddable
 */

define([], function () {

	var Embeddable = Ember.TextField.extend({
		insertNewline: function() {
			var value = this.get('value');
			if (value) {
				this.get('controller').submit();
			}
		}
	});

	Embeddable.reopenClass({
		type: 'TextField',
		kind: 'Embeddable'
	});

	return Embeddable;

});