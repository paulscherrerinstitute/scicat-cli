<script>
  export let id;
  export let value;
  export let status;
  export let progress;
</script>

<div class="listitem">
  <div class="path">
    {value}
  </div>
  <div>
    {status}
  </div>
  <div>
    {progress} %
  </div>
</div>

<style>
  .listitem {
    display: grid;
    grid-template-columns: 5fr 2fr 2fr;
    grid-template-rows: auto;
    padding-right: 30px;
  }
  .path {
    text-align: left;
  }
</style>
