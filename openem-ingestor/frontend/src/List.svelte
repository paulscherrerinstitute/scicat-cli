<script>
  export let items;
</script>

<h3>Selected Folders:</h3>
<div style="overflow-y: scroll; height:400px;">
  <ul>
    {#each Object.entries(items) as [id, item]}
      <li>
        <svelte:component this={item.component} {...item} />
      </li>
    {/each}
  </ul>
</div>

<style>
  ul {
    list-style-type: none;
  }
  li {
    display: block;
    flex: 0 1 auto; /* Default */
    list-style-type: none;
  }
</style>
